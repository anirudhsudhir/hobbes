#![deny(missing_docs)]

//! A Bitcask-like log-structured key-value store with an in-memory index

use rmp_serde::{self, decode, encode};

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Seek, SeekFrom, Write};
use std::path;
use std::{fmt, fs};

/// KV Store Error Types
#[derive(Debug)]
pub enum KvsError {
    /// Indicates I/O errors
    IoError(io::Error),
    /// Indicates errors while serializing commands to be stored in the on-disk log
    SerializationError(encode::Error),
    /// Indicates errors while deserializing commands from the on-disk log
    DeserializationError(decode::Error),
    /// Indicates absence of key in the Store
    KeyNotFoundError,
    /// Indicates CLI errors
    CliError(String),
}

/// Result type for the Store
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

/// KvStore holds the in-memory index with keys and log pointers
pub struct KvStore {
    mem_index: HashMap<String, u64>,
    db_handle: File,
    db_folder: path::PathBuf,
}

const MAX_FILE_SIZE: u64 = 10000;

impl fmt::Display for KvsError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            KvsError::IoError(ref err) => write!(f, "IO error: {}", err),
            KvsError::SerializationError(ref err) => write!(f, "Serialization error: {}", err),
            KvsError::DeserializationError(ref err) => write!(f, "Deserialization error: {}", err),
            KvsError::KeyNotFoundError => write!(f, "Key not found",),
            KvsError::CliError(ref err) => write!(f, "CLI Error: {}", err),
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
    /// Open an instance of KvStore at the specified path
    pub fn open(arg_path: &path::Path) -> Result<KvStore> {
        let mut db_file_path = arg_path.to_path_buf();
        if path::Path::is_dir(arg_path) {
            db_file_path.push("store.db");
        }

        if !path::Path::exists(&db_file_path) {
            let file = OpenOptions::new()
                .create(true)
                .read(true)
                .append(true)
                .open(&db_file_path)?;

            return Ok(KvStore {
                mem_index: HashMap::new(),
                db_handle: file,
                db_folder: arg_path.to_path_buf(),
            });
        }

        let db_writer = OpenOptions::new()
            .read(true)
            .append(true)
            .open(&db_file_path)?;
        let mut kv = KvStore {
            mem_index: HashMap::new(),
            db_handle: db_writer,
            db_folder: arg_path.to_path_buf(),
        };

        let file = File::open(&db_file_path)?;
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

        self.compaction_check()?;
        Ok(())
    }

    /// Retrieve the value associated with a key from the store
    ///
    /// ```
    /// use tempfile::TempDir;
    /// let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    ///
    /// use kvs::KvStore;
    ///
    /// let mut kv_store = KvStore::open(temp_dir.path()).expect("unable to create a new KvStore");
    /// kv_store.set("Foo".to_owned(), "Bar".to_owned()).expect("unable to set key 'Foo' to value 'Bar'");
    ///
    /// assert_eq!(kv_store.get("Foo".to_owned()).expect("unable to get key 'Foo'"), Some("Bar".to_owned()));
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
    /// use tempfile::TempDir;
    /// let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    ///
    /// use kvs::{KvStore,KvsError};
    ///
    /// let mut kv_store = KvStore::open(temp_dir.path()).expect("unable to create a new KvStore");
    /// kv_store.set("Foo".to_owned(), "Bar".to_owned()).expect("unable to set key 'Foo' to value 'Bar'");
    ///
    /// kv_store.remove("Foo".to_owned());
    ///
    /// assert_eq!(kv_store.get("Foo".to_owned()).expect("unable to get key 'Foo'"), None);
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

        self.compaction_check()?;
        Ok(())
    }

    fn compaction_manager(&mut self) -> Result<()> {
        let mut compacted_db_data: HashMap<String, String> = HashMap::new();
        let mut compacted_mem_index: HashMap<String, u64> = HashMap::new();

        let mut current_db_path = self.db_folder.clone();
        current_db_path.push("store.db");
        let current_db_file = File::open(&current_db_path)?;
        let mut current_db_reader = BufReader::new(current_db_file);

        while let Ok(decode_cmd) = decode::from_read(&mut current_db_reader) {
            let cmd: LogCommand = decode_cmd;
            match cmd.operation {
                OperationType::Set(key, value) => compacted_db_data.insert(key, value),
                OperationType::Rm(key) => compacted_db_data.remove(&key),
            };
        }
        dbg!(&compacted_db_data);

        let mut compacted_db_path = self.db_folder.clone();
        compacted_db_path.push("compacted_store.db");
        let compacted_db_file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&compacted_db_path)?;
        let mut compacted_db_writer = BufWriter::new(compacted_db_file);

        let mut offset: u64 = 0;
        for (key, val) in compacted_db_data.into_iter() {
            let cmd = serialize_command(&LogCommand {
                operation: OperationType::Set(key.clone(), val),
            })?;

            compacted_db_writer.write_all(&cmd)?;
            compacted_mem_index.insert(key, offset);
            offset = compacted_db_writer.stream_position()?;
        }

        fs::rename(&compacted_db_path, &current_db_path)?;
        let db_writer = OpenOptions::new()
            .read(true)
            .append(true)
            .open(&current_db_path)?;

        self.mem_index = compacted_mem_index;
        self.db_handle = db_writer;

        Ok(())
    }

    fn compaction_check(&mut self) -> Result<()> {
        if self.db_handle.stream_position()? >= MAX_FILE_SIZE {
            self.compaction_manager()?
        }
        Ok(())
    }
}

fn serialize_command(cmd: &LogCommand) -> Result<Vec<u8>> {
    Ok(rmp_serde::to_vec(cmd)?)
}
