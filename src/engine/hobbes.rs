use rmp_serde::{self, decode};

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use super::{Engine, KvsError, Result, HOBBES_DB_PATH, SLED_DB_PATH};

mod compaction;

#[derive(Debug, Serialize, Deserialize)]
struct LogEntry {
    key: String,
    val: String,
}

/// KvStore holds the in-memory index with keys and log pointers
#[derive(Debug)]
pub struct HobbesEngine {
    mem_index: HashMap<String, ValueMetadata>,
    logs_dir: PathBuf,
    log_writer: File,
    log_readers: HashMap<u64, BufReader<File>>,
    current_log_id: u64,
}

#[derive(Debug)]
struct ValueMetadata {
    log_pointer: u64,
    log_id: u64,
}

const TOMBSTONE: &str = "!tomb!";
const LOG_EXTENSION: &str = ".db";

impl HobbesEngine {
    /// Open an instance of HobbesEngine at the specified directory
    pub fn open(logs_dir_arg: &Path) -> Result<HobbesEngine> {
        // Check if a sled-store already exists
        let sled_store_dir = logs_dir_arg.join(SLED_DB_PATH);
        if Path::is_dir(&sled_store_dir) {
            Err(KvsError::CliError(String::from(
                "sled storage engine used previously, using the hobbes engine is an invalid operation",
            )))?
        }

        let logs_dir = logs_dir_arg.join(HOBBES_DB_PATH);

        // Check if the user-provided path is without extensions
        if Path::extension(logs_dir_arg).is_some() {
            return Err(KvsError::CliError(String::from(
                "invalid log directory path, contains an extension",
            )));
        }

        let mut log_readers = HashMap::new();
        let mut latest_file_id = 0;

        //Check if the path is a valid directory
        if Path::is_dir(&logs_dir) {
            for entry in fs::read_dir(&logs_dir)? {
                let log_path = entry?.path();
                let mut log_id_path = log_path.clone();
                log_id_path.set_extension("");

                let log_id = log_id_path
                    .strip_prefix(&logs_dir)?
                    .to_str()
                    .ok_or(KvsError::CliError(String::from(
                        "invalid log filename, {err}",
                    )))?
                    .parse::<u64>()?;

                log_readers.insert(log_id, BufReader::new(File::open(log_path)?));
                if log_id > latest_file_id {
                    latest_file_id = log_id;
                }
            }
        } else {
            fs::create_dir_all(&logs_dir)?;
        }

        let mut mem_index = HashMap::new();
        let log_writer;

        // Indicates logs are present in the directory
        if latest_file_id != 0 {
            let write_log_path =
                logs_dir.join(PathBuf::from(latest_file_id.to_string() + LOG_EXTENSION));
            log_writer = OpenOptions::new().append(true).open(&write_log_path)?;

            // Replaying logs to recreate index

            for i in 1..latest_file_id + 1 {
                let mut log_reader = log_readers.get_mut(&i).ok_or_else(|| {
                    KvsError::LogReaderNotFoundError(format!(
                        "Log {} does not have a valid reader",
                        i
                    ))
                })?;

                let mut offset = log_reader.stream_position()?;

                while let Ok(decode_cmd) = decode::from_read(&mut log_reader) {
                    let cmd: LogEntry = decode_cmd;
                    match cmd.val.as_str() {
                        TOMBSTONE => mem_index.remove(&cmd.key),
                        _ => mem_index.insert(
                            cmd.key,
                            ValueMetadata {
                                log_pointer: offset,
                                log_id: i,
                            },
                        ),
                    };

                    offset = log_reader.stream_position()?;
                }
            }
        } else {
            // Indicates no logs in directory

            let write_log_path = logs_dir.join(PathBuf::from(String::from("1") + LOG_EXTENSION));
            log_writer = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&write_log_path)?;
            log_readers.insert(1, BufReader::new(File::open(&write_log_path)?));
            latest_file_id = 1;
        }

        Ok(HobbesEngine {
            mem_index,
            logs_dir,
            log_writer,
            log_readers,
            current_log_id: latest_file_id,
        })
    }
}

impl Engine for HobbesEngine {
    /// Store a key-value pair
    fn set(&mut self, key: String, value: String) -> Result<()> {
        let cmd = serialize_command(&LogEntry {
            key: key.clone(),
            val: value.clone(),
        })?;

        self.log_writer.seek(SeekFrom::End(0))?;
        let offset = self.log_writer.stream_position()?;
        self.log_writer.write_all(&cmd)?;

        self.mem_index.insert(
            key,
            ValueMetadata {
                log_pointer: offset,
                log_id: self.current_log_id,
            },
        );

        self.compaction_check()?;
        Ok(())
    }

    /// Retrieve the value associated with a key from the store
    ///
    /// ```
    /// use tempfile::TempDir;
    /// let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    ///
    /// use hobbes_kv::engine::hobbes::HobbesEngine;
    /// use hobbes_kv::engine::Engine;
    ///
    /// let mut kv_store = HobbesEngine::open(temp_dir.path()).expect("unable to create a new KvStore");
    /// kv_store.set("Foo".to_owned(), "Bar".to_owned()).expect("unable to set key 'Foo' to value 'Bar'");
    ///
    /// assert_eq!(kv_store.get("Foo".to_owned()).expect("unable to get key 'Foo'"), Some("Bar".to_owned()));
    /// ```
    fn get(&mut self, key: String) -> Result<Option<String>> {
        // panic!("{:?}", &self);
        let value_metadata_opt = self.mem_index.get(&key);

        match value_metadata_opt {
            Some(value_metadata) => {
                let mut requested_log_reader = self
                    .log_readers
                    .get_mut(&value_metadata.log_id)
                    .ok_or_else(|| {
                        KvsError::LogReaderNotFoundError(format!(
                            "Log {} does not have a valid reader",
                            value_metadata.log_id
                        ))
                    })?;

                requested_log_reader.seek(SeekFrom::Start(value_metadata.log_pointer))?;
                let cmd: LogEntry = decode::from_read(&mut requested_log_reader)?;

                match cmd.val.as_str() {
                    TOMBSTONE => Ok(None),
                    _ => Ok(Some(cmd.val)),
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
    /// use hobbes_kv::engine::hobbes::HobbesEngine;
    /// use hobbes_kv::engine::Engine;
    ///
    /// let mut kv_store = HobbesEngine::open(temp_dir.path()).expect("unable to create a new KvStore");
    /// kv_store.set("Foo".to_owned(), "Bar".to_owned()).expect("unable to set key 'Foo' to value 'Bar'");
    ///
    /// kv_store.remove("Foo".to_owned());
    ///
    /// assert_eq!(kv_store.get("Foo".to_owned()).expect("unable to get key 'Foo'"), None);
    /// ```
    fn remove(&mut self, key: String) -> Result<()> {
        self.mem_index
            .remove(&key)
            .ok_or_else(|| KvsError::KeyNotFoundError)?;

        let cmd = serialize_command(&LogEntry {
            key,
            val: TOMBSTONE.to_string(),
        })?;

        self.log_writer.seek(SeekFrom::Start(0))?;
        self.log_writer.write_all(&cmd)?;

        self.compaction_check()?;
        Ok(())
    }
}

fn serialize_command(cmd: &LogEntry) -> Result<Vec<u8>> {
    Ok(rmp_serde::to_vec(cmd)?)
}
