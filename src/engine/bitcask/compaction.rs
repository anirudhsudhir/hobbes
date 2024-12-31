use anyhow::{anyhow, Context};
use tracing::debug;

use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::PathBuf;

use crate::engine::HOBBES_COMPACTED_LOGS_SUBPATH;
use crate::KvsError;

use super::{serialize_command, BitcaskEngine, LogEntry, Result, ValueMetadata, LOG_EXTENSION};

const MAX_FILE_SIZE: u64 = 1000000;

impl BitcaskEngine {
    fn compaction_manager(&mut self) -> Result<()> {
        debug!(operation = "COMPACTION");

        let hobbes_compacted_logs_path = self
            .db_dir
            .join(PathBuf::from(HOBBES_COMPACTED_LOGS_SUBPATH));

        fs::create_dir_all(&hobbes_compacted_logs_path)?;

        let mem_index_keys = self.mem_index.keys().cloned().collect::<Vec<String>>();

        // The updated in-memory index
        let mut updated_index = HashMap::new();

        let mut current_compact_log_id = 1;
        let mut current_compact_log_path =
            hobbes_compacted_logs_path
                .clone()
                .join(PathBuf::from(format!(
                    "{current_compact_log_id}{LOG_EXTENSION}"
                )));
        let mut current_compact_log_writer = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&current_compact_log_path).with_context(|| {
                    format!("[COMPACTION] Error while creating a new compacted log writer - log writer path -> {:?}", &current_compact_log_path)
                })?;

        let mut offset;

        // Persisting compacted logs and updating the index
        for k in mem_index_keys {
            offset = current_compact_log_writer.metadata()?.len();

            // Write to a new file if filse size threshold exceeded
            if offset >= MAX_FILE_SIZE {
                current_compact_log_id += 1;
                current_compact_log_path = hobbes_compacted_logs_path.join(PathBuf::from(format!(
                    "{current_compact_log_id}{LOG_EXTENSION}"
                )));
                current_compact_log_writer = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&current_compact_log_path).with_context(|| {
                    format!("[COMPACTION] Error while creating a new compacted log writer - log writer path -> {:?}", &current_compact_log_path)
                })?;
                offset = 0;
            }

            let (val, value_metadata) =
                self.get_val_metadata(k.clone())?
                    .ok_or(anyhow!(KvsError::CompactionError(format!(
                        "{k} present in index not found on disk while compacting!"
                    ))))?;

            // Get value of key and serialise
            let cmd = serialize_command(&LogEntry {
                key: k.clone(),
                val,
                timestamp: value_metadata.timestamp,
            })?;

            current_compact_log_writer.seek(SeekFrom::Start(offset))?;
            current_compact_log_writer.write_all(&cmd)?;

            updated_index.insert(
                k,
                ValueMetadata {
                    log_pointer: offset,
                    log_id: current_compact_log_id,
                    timestamp: value_metadata.timestamp,
                },
            );
            // debug!(
            //     operation = "COMPACTION",
            //     "compacted key \"{k}\" with value \"{val}\" to file {:?}  at offset {offset}\n getting from mem_index - {:?}",
            //     current_compact_log_path,
            //     updated_index.get(&k)
            // );
        }

        // Updating KvStore
        // TODO: Make these operations atomic
        // TODO: Handle failure when renaming compacted logs and DB crashes

        self.log_readers = None;
        // Ignoring error as directory may not exist
        let _ = fs::remove_dir_all(&self.logs_dir);

        fs::rename(&hobbes_compacted_logs_path, &self.logs_dir).with_context(|| {
            format!(
                "[COMPACTION] Error while renaming {:?} to {:?}, Current logs dir -> {:?}",
                hobbes_compacted_logs_path, self.logs_dir, self.logs_dir
            )
        })?;

        self.mem_index = updated_index;
        self.current_log_id = current_compact_log_id + 1;
        self.log_writer = None;

        Ok(())
    }

    /// Check if the current log can be compacted
    pub fn compaction_check(&mut self) -> Result<()> {
        if self.log_writer.is_none() {
            self.log_writer_init()?;
        }
        let writer_len = self.log_writer.as_mut().unwrap().metadata()?.len();
        if writer_len >= MAX_FILE_SIZE {
            self.compaction_manager()?
        }

        Ok(())
    }
}
