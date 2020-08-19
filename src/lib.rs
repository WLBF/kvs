#![deny(missing_docs)]

use std::collections::HashMap;

pub struct KvStore {
    map: HashMap<String, String>,
}

impl Default for KvStore {
    fn default() -> Self {
        Self::new()
    }
}

impl KvStore {
    pub fn new() -> Self {
        KvStore {
            map: HashMap::new(),
        }
    }

    pub fn set(&mut self, key: String, value: String) -> Option<String> {
        self.map.insert(key, value)
    }

    pub fn get(&self, key: String) -> Option<String> {
        self.map.get(&key).map(|v| v.to_owned())
    }

    pub fn remove(&mut self, key: String) -> Option<String> {
        self.map.remove(&key)
    }
}
