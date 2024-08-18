// #![deny(missing_docs)]

//! This crate is a simple in-memory key-value store

use rmp_serde::{self, decode, encode};

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, Seek, SeekFrom, Write};
use std::path;

#[derive(Debug)]
pub enum KvsError {
    IoError(io::Error),
    SerializationError(encode::Error),
    DeserializationError(decode::Error),
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
    mem_index: HashMap<String, u64>,
    db_handle: File,
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

impl From<encode::Error> for KvsError {
    fn from(value: encode::Error) -> Self {
        KvsError::SerializationError(value)
    }
}

impl From<decode::Error> for KvsError {
    fn from(value: decode::Error) -> Self {
        KvsError::DeserializationError(value)
    }
}

impl KvStore {
    /// Create an instance of KvStore
    pub fn open(arg_path: &path::Path) -> Result<KvStore> {
        let mut path = arg_path.to_path_buf();
        if path::Path::is_dir(arg_path) {
            path.push("store.db");
        }

        if !path::Path::exists(&path) {
            let file = OpenOptions::new()
                .create(true)
                .read(true)
                .append(true)
                .open(&path)?;

            return Ok(KvStore {
                mem_index: HashMap::new(),
                db_handle: file,
            });
        }

        let db_writer = OpenOptions::new().read(true).append(true).open(&path)?;
        let mut kv = KvStore {
            mem_index: HashMap::new(),
            db_handle: db_writer,
        };

        let file = File::open(&path)?;
        let mut reader = BufReader::new(&file);
        let mut offset = reader.stream_position()?;

        while let Ok(decode_cmd) = decode::from_read(&mut reader) {
            let cmd: LogCommand = decode_cmd;
            match cmd.operation {
                OperationType::Set(key, _) => kv.mem_index.insert(key, offset),
                OperationType::Rm(key) => kv.mem_index.remove(&key),
            };

            offset = reader.stream_position()?;
        }

        Ok(kv)
    }

    /// Store a key-value pair
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let cmd = serialize_command(&LogCommand {
            operation: OperationType::Set(key.clone(), value.clone()),
        })?;

        let offset = self.db_handle.stream_position()?;
        self.db_handle.write_all(&cmd)?;

        self.mem_index.insert(key, offset);
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
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        let offset_opt = self.mem_index.get(&key).copied();
        match offset_opt {
            Some(offset) => {
                self.db_handle.seek(SeekFrom::Start(offset))?;
                let cmd: LogCommand = decode::from_read(&mut self.db_handle)?;

                match cmd.operation {
                    OperationType::Set(_, val) => Ok(Some(val)),
                    OperationType::Rm(_) => Ok(None),
                }
            }
            None => Ok(None),
        }
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

        let cmd = serialize_command(&LogCommand {
            operation: OperationType::Rm(key),
        })?;

        self.db_handle.seek(SeekFrom::End(0))?;
        self.db_handle.write_all(&cmd)?;

        Ok(())
    }
}

fn serialize_command(cmd: &LogCommand) -> Result<Vec<u8>> {
    Ok(rmp_serde::to_vec(cmd)?)
}
