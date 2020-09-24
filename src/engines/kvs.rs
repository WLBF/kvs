use crate::error::{KvsError, Result};
use crate::KvsEngine;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;

const COMPACTION_THRESHOLD: u64 = 1024 * 1024;

/// The `KvStore` stores string key/value pairs.
#[derive(Clone)]
pub struct KvStore(Arc<Mutex<SharedKvStore>>);

struct SharedKvStore {
    path: PathBuf,
    current_term: u64,
    index: BTreeMap<String, Pos>,
    readers: HashMap<u64, BufReader<File>>,
    writer: BufWriter<File>,
    redundant: u64,
}

impl SharedKvStore {
    /// Open the SharedKvStore at a given path. Return the SharedKvStore.
    pub fn open(path: impl Into<PathBuf>) -> Result<SharedKvStore> {
        let path = path.into();
        let terms = sort_terms(&path)?;

        let current_term = terms.last().copied().unwrap_or(0) + 1;

        let mut redundant = 0;
        let mut readers = HashMap::new();
        let mut index = BTreeMap::new();

        readers.insert(current_term, new_reader(&path, current_term)?);

        for term in terms {
            readers.insert(term, new_reader(&path, term)?);
            redundant += load_file(term, &mut readers, &mut index)?;
        }

        let writer = new_writer(&path, current_term)?;

        Ok(SharedKvStore {
            path,
            current_term,
            index,
            readers,
            writer,
            redundant,
        })
    }

    fn compact(&mut self) -> Result<()> {
        let compact_term = self.current_term + 1;
        self.current_term += 2;

        let mut compact_writer = new_writer(&self.path, compact_term)?;
        self.writer = new_writer(&self.path, self.current_term)?;

        let compact_reader = new_reader(&self.path, compact_term)?;
        let current_reader = new_reader(&self.path, compact_term)?;
        self.readers.insert(compact_term, compact_reader);
        self.readers.insert(self.current_term, current_reader);

        let mut offset: u64 = 0;
        for pos in self.index.values_mut() {
            let reader = self
                .readers
                .get_mut(&pos.term)
                .expect("log reader not found");
            reader.seek(SeekFrom::Start(pos.offset))?;
            let mut cmd_reader = reader.take(pos.len);

            let len = io::copy(&mut cmd_reader, &mut compact_writer)?;

            *pos = Pos {
                term: compact_term,
                offset,
                len: pos.len,
            };
            offset += len;
        }
        compact_writer.flush()?;

        let stale_terms: Vec<u64> = self
            .readers
            .keys()
            .filter(|&&t| t < compact_term)
            .cloned()
            .collect();
        for term in stale_terms {
            self.readers.remove(&term);
            let path = log_path(&self.path, term);
            fs::remove_file(path)?
        }

        self.redundant = 0;
        Ok(())
    }

    /// Sets the value of a string key to a string.
    ///
    /// If the key already exists, the previous value will be overwritten.
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

        if let Some(p) = self.index.insert(key, pos) {
            self.redundant += p.len;
        }

        if self.redundant > COMPACTION_THRESHOLD {
            self.compact()?;
        }

        Ok(())
    }

    /// Gets the string value of a given string key.
    ///
    /// Returns `None` if the given key does not exist.
    fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(pos) = self.index.get(&key) {
            let reader = self
                .readers
                .get_mut(&pos.term)
                .expect("log reader not found");
            reader.seek(SeekFrom::Start(pos.offset))?;
            let cmd_reader = reader.take(pos.len);

            return match serde_json::from_reader(cmd_reader)? {
                Command::Set { key: _, value } => Ok(Some(value)),
                Command::Remove { .. } => Err(KvsError::UnexpectedCommandType),
            };
        }

        Ok(None)
    }

    /// Remove a given key
    ///
    /// Returns `KvsError::KeyNotFound` if the given key does not exist.
    fn remove(&mut self, key: String) -> Result<()> {
        if self.index.contains_key(&key) {
            let cmd = Command::Remove { key: key.clone() };
            serde_json::to_writer(&mut self.writer, &cmd)?;
            self.writer.flush()?;
            if let Some(p) = self.index.remove(&key) {
                self.redundant += p.len;
            }
            return Ok(());
        }

        if self.redundant > COMPACTION_THRESHOLD {
            self.compact()?;
        }

        Err(KvsError::KeyNotFound)
    }
}
impl KvStore {
    /// Open the KvStore at a given path. Return the KvStore.
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let inner = SharedKvStore::open(path)?;
        Ok(KvStore(Arc::new(Mutex::new(inner))))
    }
}

impl KvsEngine for KvStore {
    /// Sets the value of a string key to a string.
    ///
    /// If the key already exists, the previous value will be overwritten.
    fn set(&self, key: String, value: String) -> Result<()> {
        let mut sk = self.0.lock().unwrap();
        sk.set(key, value)
    }

    /// Gets the string value of a given string key.
    ///
    /// Returns `None` if the given key does not exist.
    fn get(&self, key: String) -> Result<Option<String>> {
        let mut sk = self.0.lock().unwrap();
        sk.get(key)
    }

    /// Removes a given key.
    ///
    /// # Errors
    ///
    /// It returns `KvsError::KeyNotFound` if the given key is not found.
    fn remove(&self, key: String) -> Result<()> {
        let mut sk = self.0.lock().unwrap();
        sk.remove(key)
    }
}

fn load_file(
    term: u64,
    readers: &mut HashMap<u64, BufReader<File>>,
    index: &mut BTreeMap<String, Pos>,
) -> Result<u64> {
    let reader = readers.get_mut(&term).expect("log reader not found");
    let mut stream = serde_json::Deserializer::from_reader(reader).into_iter::<Command>();
    let mut offset: u64 = 0;
    let mut redundant: u64 = 0;
    while let Some(res) = stream.next() {
        let new_offset = stream.byte_offset() as u64;
        let pos = Pos {
            term,
            offset,
            len: new_offset - offset,
        };
        match res? {
            Command::Set { key, .. } => index.insert(key, pos).map(|p| redundant += p.len),
            Command::Remove { key } => {
                redundant += new_offset - offset;
                index.remove(&key).map(|p| redundant += p.len)
            }
        };
        offset = new_offset;
    }

    Ok(redundant)
}

fn new_reader(dir: &Path, term: u64) -> Result<BufReader<File>> {
    let path = log_path(dir, term);

    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(&path)?;

    Ok(BufReader::new(file))
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

fn sort_terms(path: &Path) -> Result<Vec<u64>> {
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

#[derive(Debug)]
struct Pos {
    term: u64,
    offset: u64,
    len: u64,
}
