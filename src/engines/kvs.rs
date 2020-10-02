use crate::error::{KvsError, Result};
use crate::KvsEngine;
use crossbeam_skiplist::SkipMap;
use log::error;
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

const COMPACTION_THRESHOLD: u64 = 1024 * 1024;

/// The `KvStore` stores string key/value pairs.
#[derive(Clone)]
pub struct KvStore {
    path: Arc<PathBuf>,
    index: Arc<SkipMap<String, Pos>>,
    reader: KvsReader,
    writer: Arc<Mutex<KvsWriter>>,
}

impl KvStore {
    /// Opens a `KvStore` with the given path.
    ///
    /// This will create a new directory if the given one does not exist.
    ///
    /// # Errors
    ///
    /// It propagates I/O or deserialization errors during the log replay.
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path = Arc::new(path.into());
        fs::create_dir_all(&*path)?;

        let mut readers = BTreeMap::new();
        let index = Arc::new(SkipMap::new());

        let terms = sorted_terms(&path)?;
        let mut uncompacted = 0;

        for &term in &terms {
            let mut reader = BufReader::new(File::open(log_path(&path, term))?);
            uncompacted += load(term, &mut reader, &*index)?;
            readers.insert(term, reader);
        }

        let current_term = terms.last().unwrap_or(&0) + 1;
        let writer = new_writer(&path, current_term)?;
        let safe_point = Arc::new(AtomicU64::new(0));

        let reader = KvsReader {
            path: Arc::clone(&path),
            safe_point,
            readers: RefCell::new(readers),
        };

        let writer = KvsWriter {
            reader: reader.clone(),
            writer,
            current_term,
            uncompacted,
            path: Arc::clone(&path),
            index: Arc::clone(&index),
        };

        Ok(KvStore {
            path,
            reader,
            index,
            writer: Arc::new(Mutex::new(writer)),
        })
    }
}

impl KvsEngine for KvStore {
    /// Sets the value of a string key to a string.
    ///
    /// If the key already exists, the previous value will be overwritten.
    ///
    /// # Errors
    ///
    /// It propagates I/O or serialization errors during writing the log.
    fn set(&self, key: String, value: String) -> Result<()> {
        self.writer.lock().unwrap().set(key, value)
    }

    /// Gets the string value of a given string key.
    ///
    /// Returns `None` if the given key does not exist.
    fn get(&self, key: String) -> Result<Option<String>> {
        if let Some(entry) = self.index.get(&key) {
            if let Command::Set { value, .. } = self.reader.read_cmd(*entry.value())? {
                Ok(Some(value))
            } else {
                Err(KvsError::UnexpectedCommandType)
            }
        } else {
            Ok(None)
        }
    }

    /// Removes a given key.
    ///
    /// # Error
    ///
    /// It returns `KvsError::KeyNotFound` if the given key is not found.
    ///
    /// It propagates I/O or serialization errors during writing the log.
    fn remove(&self, key: String) -> Result<()> {
        self.writer.lock().unwrap().remove(key)
    }
}

fn load(term: u64, reader: &mut BufReader<File>, index: &SkipMap<String, Pos>) -> Result<u64> {
    let mut offset: u64 = reader.seek(SeekFrom::Start(0))?;
    let mut stream = serde_json::Deserializer::from_reader(reader).into_iter::<Command>();
    let mut uncompacted: u64 = 0;
    while let Some(res) = stream.next() {
        let new_offset = stream.byte_offset() as u64;
        let pos = Pos {
            term,
            offset,
            len: new_offset - offset,
        };
        match res? {
            Command::Set { key, .. } => {
                if let Some(entry) = index.get(&key) {
                    uncompacted += entry.value().len;
                }
                index.insert(key, pos);
            }
            Command::Remove { key } => {
                if let Some(entry) = index.remove(&key) {
                    uncompacted += entry.value().len;
                }
                uncompacted += new_offset - offset;
            }
        };
        offset = new_offset;
    }

    Ok(uncompacted)
}

fn new_writer(dir: &Path, term: u64) -> Result<BufWriter<File>> {
    let path = log_path(dir, term);

    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(&path)?;

    Ok(BufWriter::new(file))
}

fn sorted_terms(path: &Path) -> Result<Vec<u64>> {
    let mut terms = fs::read_dir(path)?
        .flat_map(|res| {
            res.expect("log file error")
                .path()
                .file_stem()
                .map(|s| s.to_owned())
        })
        .flat_map(|o| o.to_str().map(|s| s.to_owned()))
        .flat_map(|s| s.parse::<u64>())
        .collect::<Vec<u64>>();

    terms.sort();

    Ok(terms)
}

fn log_path(dir: &Path, term: u64) -> PathBuf {
    dir.join(format!("{}.log", term))
}

#[derive(Serialize, Deserialize, Debug)]
enum Command {
    Set { key: String, value: String },
    Remove { key: String },
}

#[derive(Debug, Clone, Copy)]
struct Pos {
    term: u64,
    offset: u64,
    len: u64,
}

struct KvsReader {
    path: Arc<PathBuf>,
    safe_point: Arc<AtomicU64>,
    readers: RefCell<BTreeMap<u64, BufReader<File>>>,
}

impl Clone for KvsReader {
    fn clone(&self) -> KvsReader {
        KvsReader {
            path: Arc::clone(&self.path),
            safe_point: Arc::clone(&self.safe_point),
            readers: RefCell::new(BTreeMap::new()),
        }
    }
}

impl KvsReader {
    fn close_stale_handles(&self) {
        let mut readers = self.readers.borrow_mut();
        while !readers.is_empty() {
            let term = *readers.keys().next().unwrap();
            if self.safe_point.load(Ordering::SeqCst) <= term {
                break;
            }
            readers.remove(&term);
        }
    }

    fn read_and<F, R>(&self, pos: Pos, f: F) -> Result<R>
    where
        F: FnOnce(io::Take<&mut BufReader<File>>) -> Result<R>,
    {
        self.close_stale_handles();

        let mut readers = self.readers.borrow_mut();
        if !readers.contains_key(&pos.term) {
            let file = File::open(log_path(&self.path, pos.term))?;
            let reader = BufReader::new(file);
            readers.insert(pos.term, reader);
        }

        let reader = readers.get_mut(&pos.term).unwrap();
        reader.seek(SeekFrom::Start(pos.offset))?;
        let cmd_reader = reader.take(pos.len);
        f(cmd_reader)
    }

    fn read_cmd(&self, pos: Pos) -> Result<Command> {
        self.read_and(pos, |cmd_reader| Ok(serde_json::from_reader(cmd_reader)?))
    }
}

struct KvsWriter {
    path: Arc<PathBuf>,
    current_term: u64,
    uncompacted: u64,
    reader: KvsReader,
    writer: BufWriter<File>,
    index: Arc<SkipMap<String, Pos>>,
}

impl KvsWriter {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        let cmd = Command::Set {
            key: key.clone(),
            value,
        };
        let offset = self.writer.seek(SeekFrom::Current(0))?;
        serde_json::to_writer(&mut self.writer, &cmd)?;
        self.writer.flush()?;
        let new_offset = self.writer.seek(SeekFrom::Current(0))?;
        let pos = Pos {
            term: self.current_term,
            offset,
            len: new_offset - offset,
        };

        if let Some(entry) = self.index.get(&key) {
            self.uncompacted += entry.value().len;
        }
        self.index.insert(key, pos);

        if self.uncompacted > COMPACTION_THRESHOLD {
            self.compact()?;
        }

        Ok(())
    }

    fn remove(&mut self, key: String) -> Result<()> {
        if self.index.contains_key(&key) {
            let cmd = Command::Remove { key: key.clone() };
            serde_json::to_writer(&mut self.writer, &cmd)?;
            self.writer.flush()?;
            if let Some(entry) = self.index.remove(&key) {
                self.uncompacted += entry.value().len;
            }
            return Ok(());
        }

        if self.uncompacted > COMPACTION_THRESHOLD {
            self.compact()?;
        }

        Err(KvsError::KeyNotFound)
    }

    fn compact(&mut self) -> Result<()> {
        let compact_term = self.current_term + 1;
        self.current_term += 2;

        let mut compact_writer = new_writer(&self.path, compact_term)?;
        self.writer = new_writer(&self.path, self.current_term)?;

        let mut offset: u64 = 0;
        for entry in self.index.iter() {
            let len = self.reader.read_and(*entry.value(), |mut entry_reader| {
                Ok(io::copy(&mut entry_reader, &mut compact_writer)?)
            })?;

            let new_pos = Pos {
                term: compact_term,
                offset,
                len: entry.value().len,
            };
            self.index.insert(entry.key().clone(), new_pos);

            offset += len;
        }
        compact_writer.flush()?;

        self.reader.safe_point.store(compact_term, Ordering::SeqCst);
        self.reader.close_stale_handles();

        let stale_terms = sorted_terms(&self.path)?
            .into_iter()
            .filter(|&gen| gen < compact_term);

        for term in stale_terms {
            let path = log_path(&self.path, term);
            if let Err(e) = fs::remove_file(&path) {
                error!("{:?} cannot be deleted: {}", path, e);
            }
        }

        self.uncompacted = 0;
        Ok(())
    }
}
