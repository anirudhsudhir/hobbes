use tracing::{debug, error};

use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::PathBuf;

use crate::engine::BITCASK_COMPACTED_LOGS_SUBPATH;
use crate::{HobbesError, MUTEX_LOCK_ERROR};

use super::{serialize_command, BitcaskEngine, LogEntry, Result, ValueMetadata, LOG_EXTENSION};

const MAX_FILE_SIZE: u64 = 1000000;

impl BitcaskEngine {
    pub fn compaction_manager(&self) -> Result<()> {
        debug!(operation = "COMPACTION");

        let store_mutex = self.store.clone();
        let mut bitcask_store = store_mutex.lock().expect(MUTEX_LOCK_ERROR);

        if bitcask_store.log_writer.is_none() {
            bitcask_store.log_writer_init()?;
        }
        let writer_len = bitcask_store.log_writer.as_mut().unwrap().metadata()?.len();
        if writer_len < MAX_FILE_SIZE {
            return Ok(());
        }

        let bitcask_compacted_logs_path = bitcask_store
            .db_dir
            .join(PathBuf::from(BITCASK_COMPACTED_LOGS_SUBPATH));

        fs::create_dir_all(&bitcask_compacted_logs_path)?;

        let mem_index_keys = bitcask_store
            .mem_index
            .keys()
            .cloned()
            .collect::<Vec<String>>();

        drop(bitcask_store);

        // The updated in-memory index
        let mut updated_index = HashMap::new();

        let mut current_compact_log_id = 1;
        let mut current_compact_log_path =
            bitcask_compacted_logs_path
                .clone()
                .join(PathBuf::from(format!(
                    "{current_compact_log_id}{LOG_EXTENSION}"
                )));
        let mut current_compact_log_writer = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&current_compact_log_path).map_err(|e| {
                    error!("[COMPACTION] Error while creating a new compacted log writer - log writer path -> {:?}", &current_compact_log_path);
                    HobbesError::IoError(e)

                })?;

        let mut offset;

        // Persisting compacted logs and updating the index
        for k in mem_index_keys {
            offset = current_compact_log_writer.metadata()?.len();

            // Write to a new file if filse size threshold exceeded
            if offset >= MAX_FILE_SIZE {
                current_compact_log_id += 1;
                current_compact_log_path = bitcask_compacted_logs_path.join(PathBuf::from(
                    format!("{current_compact_log_id}{LOG_EXTENSION}"),
                ));
                current_compact_log_writer = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&current_compact_log_path).map_err(|e| {
                    error!("[COMPACTION] Error while creating a new compacted log writer - log writer path -> {:?}", &current_compact_log_path);
                    HobbesError::IoError(e)

                })?;
                offset = 0;
            }

            let (val, value_metadata) =
                self.get_val_metadata(k.clone())?
                    .ok_or(HobbesError::CompactionError(format!(
                        "{k} present in index not found on disk while compacting!"
                    )))?;

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

        let mut bitcask_store = store_mutex.lock().expect(MUTEX_LOCK_ERROR);

        bitcask_store.log_readers = None;
        // Ignoring error as directory may not exist
        let _ = fs::remove_dir_all(&bitcask_store.logs_dir);

        fs::rename(&bitcask_compacted_logs_path, &bitcask_store.logs_dir).map_err(|e| {
            error!(
                "[COMPACTION] Error while renaming {:?} to {:?}, Current logs dir -> {:?}",
                bitcask_compacted_logs_path, bitcask_store.logs_dir, bitcask_store.logs_dir
            );
            HobbesError::IoError(e)
        })?;

        bitcask_store.mem_index = updated_index;
        bitcask_store.current_log_id = current_compact_log_id + 1;
        bitcask_store.log_writer = None;

        Ok(())
    }
}
