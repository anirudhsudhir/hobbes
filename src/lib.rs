// #![deny(missing_docs)]

//! This crate is a simple in-memory key-value store

// use flexbuffers::FlexbufferSerializer;

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{self, Cursor, Read, Write};
use std::path::{self, PathBuf};

#[derive(Debug)]
pub enum KvsError {
    IoError(io::Error),
    // SerializationError(flexbuffers::SerializationError),
    // DeserializationError(flexbuffers::DeserializationError),
    SerializationError(bson::ser::Error),
    DeserializationError(bson::de::Error),
    MapError(String),
    KeyNotFoundError,
    CliError(String),
    OtherError(String),
}

pub type Result<T> = std::result::Result<T, KvsError>;

#[derive(Debug, Serialize, Deserialize)]
enum OperationType {
    Set(String, String),
    Rm(String),
}

#[derive(Debug, Serialize, Deserialize)]
struct LogCommand {
    operation: OperationType,
}

/// KvStore holds a HashMap that stores the key-value pairs
pub struct KvStore {
    mem_index: HashMap<String, String>,
    db_path: PathBuf,
}

impl fmt::Display for KvsError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            KvsError::IoError(ref err) => write!(f, "IO error: {}", err),
            KvsError::SerializationError(ref err) => write!(f, "Serialization error: {}", err),
            KvsError::DeserializationError(ref err) => write!(f, "Deserialization error: {}", err),
            KvsError::MapError(ref err) => write!(f, "In-memory map error: {}", err),
            KvsError::KeyNotFoundError => write!(f, "Key not found",),
            KvsError::CliError(ref err) => write!(f, "CLI Error: {}", err),
            KvsError::OtherError(ref err) => write!(f, "Other Error: {}", err),
        }
    }
}

impl From<std::io::Error> for KvsError {
    fn from(value: std::io::Error) -> Self {
        KvsError::IoError(value)
    }
}

// impl From<flexbuffers::SerializationError> for KvsError {
//     fn from(value: flexbuffers::SerializationError) -> Self {
//         KvsError::SerializationError(value)
//     }
// }
//
// impl From<flexbuffers::DeserializationError> for KvsError {
//     fn from(value: flexbuffers::DeserializationError) -> Self {
//         KvsError::DeserializationError(value)
//     }
// }

impl From<bson::ser::Error> for KvsError {
    fn from(value: bson::ser::Error) -> Self {
        KvsError::SerializationError(value)
    }
}

impl From<bson::de::Error> for KvsError {
    fn from(value: bson::de::Error) -> Self {
        KvsError::DeserializationError(value)
    }
}

impl KvStore {
    /// Create an instance of KvStore
    pub fn open(arg_path: &path::Path) -> Result<KvStore> {
        let mut cmds: Vec<LogCommand> = Vec::new();

        let mut path = arg_path.to_path_buf();
        if path::Path::is_dir(arg_path) {
            path.push("store.db");
        }

        if !path::Path::exists(&path) {
            File::create_new(&path)?;

            return Ok(KvStore {
                mem_index: HashMap::new(),
                db_path: path,
            });
        }

        let mut file = File::open(&path)?;
        let mut buf: Vec<u8> = Vec::new();
        file.read_to_end(&mut buf)?;
        let mut reader = Cursor::new(buf);

        while let Ok(doc) = bson::Document::from_reader(&mut reader) {
            cmds.push(bson::from_document(doc)?);
        }

        let mut kv = KvStore {
            mem_index: HashMap::new(),
            db_path: path,
        };

        for cmd in cmds {
            match cmd.operation {
                OperationType::Set(key, value) => kv.mem_index.insert(key, value),
                OperationType::Rm(key) => kv.mem_index.remove(&key),
            };
        }

        Ok(kv)
    }

    /// Store a key-value pair
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let command = LogCommand {
            operation: OperationType::Set(key.clone(), value.clone()),
        };
        // let mut serializer = FlexbufferSerializer::new();
        // command.serialize(&mut serializer)?;

        let cmd = bson::to_vec(&command)?;

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.db_path)?;
        file.write_all(&cmd)?;

        self.mem_index.insert(key, value);

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
        Ok(self.mem_index.get(&key).cloned())
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
        self.mem_index
            .remove(&key)
            .ok_or_else(|| KvsError::KeyNotFoundError)?;

        let command = LogCommand {
            operation: OperationType::Rm(key),
        };
        // let mut serializer = FlexbufferSerializer::new();
        // command.serialize(&mut serializer)?;

        let cmd = bson::to_vec(&command)?;

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.db_path)?;
        file.write_all(&cmd)?;

        Ok(())
    }
}
