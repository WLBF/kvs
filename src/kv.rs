use crate::error::{KvsError, Result};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, BTreeMap};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Seek, SeekFrom, Write, Read};
use std::path::{Path, PathBuf};

const COMPACTION_THRESHOLD: u64 = 1024 * 1024;

/// The `KvStore` stores string key/value pairs.
///
pub struct KvStore {
    path: PathBuf,
    current_term: u64,
    index: BTreeMap<String, Pos>,
    readers: HashMap<u64, File>,
    writer: File,
    redundant: u64,
}

impl KvStore {
    /// Open the KvStore at a given path. Return the KvStore.
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path = path.into();
        let terms = sort_terms(&path)?;

        let current_term = terms.last().map(|t| *t).unwrap_or(0) + 1;

        let mut redundant = 0;
        let mut readers = HashMap::new();
        let mut index = BTreeMap::new();

        readers.insert(current_term, new_reader(&path, current_term)?);

        for term in terms {
            readers.insert(term, new_reader(&path, term)?);
            redundant += load_file(term, &mut readers, &mut index)?;
        }

        let writer = new_writer(&path, current_term)?;

        Ok(KvStore {
            path,
            current_term,
            index,
            readers,
            writer,
            redundant,
        })
    }

    /// Sets the value of a string key to a string.
    ///
    /// If the key already exists, the previous value will be overwritten.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let cmd = Command::Set {
            key: key.clone(),
            value,
        };
        let offset = self.writer.seek(SeekFrom::Current(0))?;
        serde_json::to_writer(&mut self.writer, &cmd)?;
        self.writer.flush()?;
        let new_offset = self.writer.seek(SeekFrom::Current(0))?;
        let pos = Pos { term: self.current_term, offset, len: new_offset - offset };
        self.index.insert(key, pos).map(|p| self.redundant += p.len);

        if self.redundant > COMPACTION_THRESHOLD {
            self.compact()?;
        }

        Ok(())
    }

    /// Gets the string value of a given string key.
    ///
    /// Returns `None` if the given key does not exist.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(pos) = self.index.get(&key) {
            let reader = self.readers.get_mut(&pos.term).expect("log reader not found");
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
    pub fn remove(&mut self, key: String) -> Result<()> {
        if self.index.contains_key(&key) {
            let cmd = Command::Remove { key: key.clone() };
            serde_json::to_writer(&self.writer, &cmd)?;
            self.writer.flush()?;
            self.index.remove(&key).map(|p| self.redundant += p.len);
            return Ok(());
        }

        if self.redundant > COMPACTION_THRESHOLD {
            self.compact()?;
        }

        Err(KvsError::KeyNotFound)
    }

    fn compact(&mut self) -> Result<()> {
        let compact_term = self.current_term + 1;
        self.current_term += 2;

        println!("compact {}", compact_term);

        let mut compact_writer = new_writer(&self.path, compact_term)?;
        self.writer = new_writer(&self.path, self.current_term)?;

        let compact_reader = new_reader(&self.path, compact_term)?;
        let current_reader = new_reader(&self.path, compact_term)?;
        self.readers.insert(compact_term, compact_reader);
        self.readers.insert(self.current_term, current_reader);

        let mut offset: u64 = 0;
        for pos in self.index.values_mut() {

            let reader = self.readers.get_mut(&pos.term).expect("log reader not found");
            reader.seek(SeekFrom::Start(pos.offset))?;
            let mut cmd_reader = reader.take(pos.len);

            let len = io::copy(&mut cmd_reader, &mut compact_writer)?;

            *pos = Pos { term: compact_term, offset, len: pos.len };
            offset += len;
        }
        compact_writer.flush()?;

        let stale_terms: Vec<u64> = self.readers.keys().filter(|&&t| t < compact_term).cloned().collect();
        for term in stale_terms {
            self.readers.remove(&term);
            let path = log_path(&self.path, term);
            fs::remove_file(path)?
        }

        self.redundant = 0;
        Ok(())
    }
}

fn load_file(term: u64, readers: &mut HashMap<u64, File>, index: &mut BTreeMap<String, Pos>) -> Result<u64> {
    let reader = readers.get_mut(&term).expect("log reader not found");
    let mut stream = serde_json::Deserializer::from_reader(reader).into_iter::<Command>();
    let mut offset: u64 = 0;
    let mut redundant: u64 = 0;
    while let Some(res) = stream.next() {
        let new_offset = stream.byte_offset() as u64;
        let pos = Pos { term, offset, len: new_offset - offset };
        match res? {
            Command::Set { key, .. } => index.insert(key, pos).map(|p| redundant += p.len),
            Command::Remove { key } => {
                redundant += new_offset - offset;
                index.remove(&key).map(|p| redundant += p.len)
            },
        };
        offset = new_offset;
    }

    Ok(redundant)
}

fn new_reader(dir: &Path, term: u64) -> Result<File> {
    let path = log_path(dir, term);

    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(&path)?;

    Ok(file)
}

fn new_writer(dir: &Path, term: u64) -> Result<File> {
    let path = log_path(dir, term);

    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(&path)?;

    Ok(file)
}

fn sort_terms(path: &Path) -> Result<Vec<u64>> {
    let mut terms = fs::read_dir(path)?
        .flat_map(|res| res.expect("log file error").path().file_stem().map(|s| s.to_owned()))
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
