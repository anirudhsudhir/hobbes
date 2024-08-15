// #![deny(missing_docs)]

//! This crate is a simple in-memory key-value store

use std::collections::HashMap;

/// KvStore holds a HashMap that stores the key-value pairs
pub struct KvStore {
    store: HashMap<String, String>,
}

#[derive(Debug)]
pub enum KvsError {
    Error,
}

pub type Result<T> = std::result::Result<T, KvsError>;

impl KvStore {
    /// Create an instance of KvStore
    pub fn open(path: &std::path::Path) -> Result<KvStore> {
        Ok(KvStore {
            store: HashMap::new(),
        })
    }

    /// Store a key-value pair
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        self.store.insert(key, value);
        Ok(())
    }

    /// Retrieve the value associated with a key from the store
    ///
    /// ```
    /// use kvs::KvStore;
    ///
    /// let mut kv_store = KvStore::new();
    /// kv_store.set("Foo".to_owned(), "Bar".to_owned());
    ///
    /// assert_eq!(kv_store.get("Foo".to_owned()), Some("Bar".to_owned()));
    /// ```
    pub fn get(&self, key: String) -> Result<Option<String>> {
        Ok(self.store.get(&key).cloned())
    }

    /// Delete a key-value pair from the store
    ///
    /// ```
    /// use kvs::KvStore;
    ///
    /// let mut kv_store = KvStore::new();
    /// kv_store.set("Foo".to_owned(), "Bar".to_owned());
    ///
    /// kv_store.remove("Foo".to_owned());
    /// assert_eq!(kv_store.get("Foo".to_owned()), None);
    /// ```
    pub fn remove(&mut self, key: String) -> Result<()> {
        self.store.remove(&key);
        Ok(())
    }
}
