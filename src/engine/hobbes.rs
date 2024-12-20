use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Local};
use rmp_serde::{self, decode};
use tracing::trace;
use tracing_subscriber::fmt::time;
use tracing_subscriber::FmtSubscriber;

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::engine::HOBBES_DB_PATH;

use super::{Engine, KvsError, HOBBES_LOGS_PATH, SLED_DB_PATH};

mod compaction;

#[derive(Debug, Serialize, Deserialize, Clone)]
struct LogEntry {
    key: String,
    val: String,
    timestamp: DateTime<Local>,
}

/// KvStore holds the in-memory index with keys and log pointers
#[derive(Debug)]
pub struct HobbesEngine {
    mem_index: HashMap<String, ValueMetadata>,
    logs_dir: PathBuf,
    db_dir: PathBuf,
    log_writer: Option<File>,
    log_readers: Option<HashMap<u64, BufReader<File>>>,
    current_log_id: u64,
}

#[derive(Debug, Clone)]
struct ValueMetadata {
    log_pointer: u64,
    log_id: u64,
    timestamp: DateTime<Local>,
}

const TOMBSTONE: &str = "!tomb!";
const LOG_EXTENSION: &str = ".db";

impl HobbesEngine {
    /// Open an instance of HobbesEngine at the specified directory
    pub fn open(logs_dir_arg: &Path) -> Result<HobbesEngine> {
        let logging_level = match env::var("LOG_LEVEL") {
            Ok(level) => match level.as_str() {
                "TRACE" => tracing::Level::TRACE,
                "DEBUG" => tracing::Level::DEBUG,
                "INFO" => tracing::Level::INFO,
                "WARN" => tracing::Level::WARN,
                "ERROR" => tracing::Level::ERROR,
                _ => tracing::Level::INFO,
            },
            Err(_) => tracing::Level::INFO,
        };

        let subscriber = FmtSubscriber::builder()
            .with_max_level(logging_level)
            .with_timer(time::ChronoLocal::rfc_3339())
            .with_target(true)
            .with_writer(std::io::stderr)
            .finish();

        let _ = tracing::subscriber::set_global_default(subscriber);

        // Check if a sled-store already exists
        let sled_store_dir = logs_dir_arg.join(SLED_DB_PATH);
        if Path::is_dir(&sled_store_dir) {
            Err(anyhow!(KvsError::CliError(String::from(
                "sled storage engine used previously, using the hobbes engine is an invalid operation",
            ))))?
        }

        let logs_dir = logs_dir_arg.join(HOBBES_LOGS_PATH);
        let db_dir = logs_dir_arg.join(HOBBES_DB_PATH);

        // Check if the user-provided path is without extensions
        if Path::extension(logs_dir_arg).is_some() {
            return Err(anyhow!(KvsError::CliError(String::from(
                "invalid log directory path, contains an extension",
            ))));
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
                    .ok_or(anyhow!(KvsError::CliError(String::from(
                        "invalid log filename, {err}",
                    ))))?
                    .parse::<u64>()?;

                log_readers.insert(
                    log_id,
                    BufReader::new(File::open(&log_path).with_context(|| {
                        format!("[DB_INIT] Error while initialising log readers - log reader path -> {:?}", &log_path)
                    })?),
                );
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
            log_writer = OpenOptions::new()
                .append(true)
                .open(&write_log_path)
                .with_context(|| {
                    format!("[DB_INIT] Error while opening an existing mutable append log - log writer path -> {:?}", write_log_path)
                })?;

            // Replaying logs to recreate index

            for (i, mut log_reader) in log_readers.iter_mut() {
                let mut offset = 0;
                log_reader.seek(SeekFrom::Start(0))?;

                while let Ok(decode_cmd) = decode::from_read(&mut log_reader) {
                    let cmd: LogEntry = decode_cmd;

                    if let Some(mem_cmd) = mem_index.get(&cmd.key) {
                        let mem_cmd: &ValueMetadata = mem_cmd;

                        if cmd.timestamp < mem_cmd.timestamp {
                            offset = log_reader.stream_position()?;
                            continue;
                        }
                    }

                    match cmd.val.as_str() {
                        TOMBSTONE => mem_index.remove(&cmd.key),
                        _ => mem_index.insert(
                            cmd.key,
                            ValueMetadata {
                                log_pointer: offset,
                                log_id: i.to_owned(),
                                timestamp: cmd.timestamp,
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
                .open(&write_log_path)
                .with_context(|| {
                    format!("[DB_INIT] Error while creating a new mutable append log - log writer path -> {:?}", write_log_path)
                })?;
            log_readers.insert(1, BufReader::new(File::open(&write_log_path)
                .with_context(|| {
                    format!("[DB_INIT] Error while creating a reader for the new mutable append log created - log reader path -> {:?}", write_log_path)
                })?));
            latest_file_id = 1;
        }

        Ok(HobbesEngine {
            mem_index,
            logs_dir,
            db_dir,
            log_writer: Some(log_writer),
            log_readers: Some(log_readers),
            current_log_id: latest_file_id,
        })
    }
}

impl Engine for HobbesEngine {
    /// Store a key-value pair
    fn set(&mut self, key: String, value: String) -> Result<()> {
        trace!(operation = "SET", key = key, value = value);

        let cmd = serialize_command(&LogEntry {
            key: key.clone(),
            val: value.clone(),
            timestamp: Local::now(),
        })?;

        if self.log_writer.is_none() {
            self.log_writer_init()?;
        }

        let log_writer = self.log_writer.as_mut().unwrap();

        let offset = log_writer.metadata()?.len();

        log_writer.seek(SeekFrom::Start(offset))?;
        log_writer.write_all(&cmd)?;

        self.mem_index.insert(
            key,
            ValueMetadata {
                log_pointer: offset,
                log_id: self.current_log_id,
                timestamp: Local::now(),
            },
        );

        // let get_val = self.get(key.clone())?;
        // trace!(
        //     operation = "SET",
        //     key = key,
        //     value = value,
        //     "\n\n key as bytes = {:?} \n added_to_mem_index => log_pointer = {offset} log_id = {} \n retrieving from mem_index = {:?} \n performing a get on the key = {:?} \n\n",
        //     key.as_bytes(),
        //     self.current_log_id,
        //     self.mem_index.get(&key),
        //     get_val
        // );

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
        // trace!(operation = "GET", key = key);
        match self.get_val_metadata(key)? {
            Some((val, _)) => Ok(Some(val)),
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
        // trace!(operation = "RM", key = key);

        self.mem_index
            .remove(&key)
            .ok_or_else(|| anyhow!(KvsError::KeyNotFoundError))?;

        let cmd = serialize_command(&LogEntry {
            key,
            val: TOMBSTONE.to_string(),
            timestamp: Local::now(),
        })?;

        if self.log_writer.is_none() {
            self.log_writer_init()?;
        }

        let log_writer = self.log_writer.as_mut().unwrap();
        let offset = log_writer.metadata()?.len();

        log_writer.seek(SeekFrom::Start(offset))?;
        log_writer.write_all(&cmd)?;

        self.compaction_check()?;
        Ok(())
    }
}

impl HobbesEngine {
    fn log_writer_init(&mut self) -> Result<()> {
        if self.log_writer.is_none() {
            trace!(operation = "LOG_WRITER_INIT");

            let write_log_path = self.logs_dir.join(PathBuf::from(format!(
                "{}{LOG_EXTENSION}",
                self.current_log_id
            )));

            self.log_writer = Some(
                fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&write_log_path).with_context(|| {
                    format!("[LOG_WRITER_INIT] Error while creating a new mutable append log - log writer path -> {:?}", write_log_path)
                })?

            );

            if self.log_readers.is_none() {
                self.log_readers_init()?;
            }

            self.log_readers.as_mut().unwrap().insert(
                self.current_log_id,
                BufReader::new(fs::File::open(&write_log_path).with_context(|| {
                    format!("[LOG_WRITER_INIT] Error while creating a reader for the new mutable append log - log reader path -> {:?}", write_log_path)
                })?),
            );
        }

        Ok(())
    }

    fn log_readers_init(&mut self) -> Result<()> {
        if self.log_readers.is_none() {
            trace!(operation = "LOG_READERS_INIT");

            let mut readers = HashMap::new();
            for entry in fs::read_dir(&self.logs_dir)? {
                let log_path = entry?.path();
                let mut log_id_path = log_path.clone();
                log_id_path.set_extension("");

                let log_id = log_id_path
                    .strip_prefix(&self.logs_dir)?
                    .to_str()
                    .ok_or(anyhow!(KvsError::CliError(String::from(
                        "invalid log filename, {err}",
                    ))))?
                    .parse::<u64>()?;

                readers.insert(log_id, BufReader::new(File::open(&log_path).with_context(|| {
                    format!("[LOG_READERS_INIT] Error while creating a new reader - log reader path -> {:?}", &log_path)
                })?));
            }

            self.log_readers = Some(readers);
        }
        Ok(())
    }

    fn get_val_metadata(&mut self, key: String) -> Result<Option<(String, ValueMetadata)>> {
        if self.log_readers.is_none() {
            self.log_readers_init()?;
        }
        let value_metadata_opt = self.mem_index.get(&key);

        match value_metadata_opt {
            Some(value_metadata) => {
                let mut requested_log_reader = self
                    .log_readers
                    .as_mut()
                    .unwrap()
                    .get_mut(&value_metadata.log_id)
                    .ok_or_else(|| {
                        anyhow!(KvsError::LogReaderNotFoundError(format!(
                            "Log {} does not have a valid reader",
                            value_metadata.log_id
                        )))
                    })?;

                requested_log_reader.seek(SeekFrom::Start(value_metadata.log_pointer))?;
                let cmd: LogEntry = decode::from_read(&mut requested_log_reader)?;

                match cmd.val.as_str() {
                    TOMBSTONE => Ok(None),
                    _ => Ok(Some((cmd.val, value_metadata.to_owned()))),
                }
            }
            None => Ok(None),
        }
    }
}

fn serialize_command(cmd: &LogEntry) -> Result<Vec<u8>> {
    Ok(rmp_serde::to_vec(cmd)?)
}
