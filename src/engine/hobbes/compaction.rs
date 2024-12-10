use rmp_serde::decode;
use tracing::debug;

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, Seek, SeekFrom, Write};
use std::path::PathBuf;

use crate::engine::{Engine, HOBBES_COMPACTED_LOGS_PATH, HOBBES_DB_PATH};
use crate::KvsError;

use super::{serialize_command, HobbesEngine, LogEntry, Result, ValueMetadata, LOG_EXTENSION};

const MAX_FILE_SIZE: u64 = 100;

impl HobbesEngine {
    fn compaction_manager(&mut self) -> Result<()> {
        fs::create_dir_all(HOBBES_COMPACTED_LOGS_PATH)?;

        let logs_dir_copy = self.logs_dir.clone();
        let compacted_logs_dir = PathBuf::from(HOBBES_COMPACTED_LOGS_PATH);
        let mem_index_keys = self
            .mem_index
            .keys()
            .map(|k| k.clone())
            .collect::<Vec<String>>();

        // The updated in-memory index
        let mut updated_index = HashMap::new();

        // Store compacted log ids, names and the corresponding valid names for rename
        let mut compacted_file_names: Vec<(u64, PathBuf)> = Vec::new();

        let mut current_compact_log_id = 1;
        let mut current_compact_log_path = compacted_logs_dir.join(PathBuf::from(format!(
            "{current_compact_log_id}{LOG_EXTENSION}"
        )));
        let mut current_compact_log_writer = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&current_compact_log_path)?;
        compacted_file_names.push((
            current_compact_log_id,
            logs_dir_copy.join(PathBuf::from(format!(
                "{current_compact_log_id}{LOG_EXTENSION}"
            ))),
        ));

        let mut offset;

        // Persisting compacted logs and updating the index
        for k in mem_index_keys {
            offset = current_compact_log_writer.metadata()?.len();
            debug!(
                " \n\n COMPACTION key - {k};  file offset {offset}; file stream_position {} \n",
                current_compact_log_writer.stream_position()?
            );

            // Write to a new file if filse size threshold exceeded
            if offset >= MAX_FILE_SIZE {
                current_compact_log_id += 1;
                current_compact_log_path = compacted_logs_dir.join(PathBuf::from(format!(
                    "{current_compact_log_id}{LOG_EXTENSION}"
                )));
                current_compact_log_writer = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&current_compact_log_path)?;
                compacted_file_names.push((
                    current_compact_log_id,
                    logs_dir_copy.join(PathBuf::from(format!(
                        "{current_compact_log_id}{LOG_EXTENSION}"
                    ))),
                ));
                offset = 0;
            }

            let val = self
                .get(k.clone())?
                .ok_or(KvsError::CompactionError(format!(
                    "{k} present in index not found on disk while compacting!"
                )))?;

            // Get value of key and serialise
            let cmd = serialize_command(&LogEntry {
                key: k.clone(),
                val,
            })?;

            current_compact_log_writer.seek(SeekFrom::Start(offset))?;
            current_compact_log_writer.write_all(&cmd)?;

            let deserialised_cmd: LogEntry = rmp_serde::from_slice(cmd.as_slice())?;
            let mut file = fs::File::open(&current_compact_log_path)?;
            file.seek(SeekFrom::Start(offset))?;
            let decoded_cmd: LogEntry = decode::from_read(file)?;
            debug!(
                "serialize_command {:?} len {} \n deserialised_command {:?} \n decoded_cmd {:?}",
                cmd,
                cmd.len(),
                deserialised_cmd,
                decoded_cmd
            );

            updated_index.insert(
                k,
                ValueMetadata {
                    log_pointer: offset,
                    log_id: current_compact_log_id,
                },
            );
        }

        // Updating KvStore
        // TODO: Make these operations atomic
        // TODO: Handle failure when renaming compacted logs and DB crashes

        self.log_readers.clear();
        fs::remove_dir_all(HOBBES_DB_PATH)?;

        fs::rename(HOBBES_COMPACTED_LOGS_PATH, HOBBES_DB_PATH)?;

        for (compacted_log_id, file_path) in compacted_file_names {
            self.log_readers
                .insert(compacted_log_id, BufReader::new(File::open(&file_path)?));
        }
        self.mem_index = updated_index;

        // Stopping writes to the current log and creating a new log
        let new_log_path = logs_dir_copy.join(PathBuf::from(
            (current_compact_log_id + 1).to_string() + LOG_EXTENSION,
        ));

        self.current_log_id = current_compact_log_id + 1;
        self.log_writer = Some(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(&new_log_path)?,
        );

        self.log_readers.insert(
            self.current_log_id,
            BufReader::new(File::open(&new_log_path)?),
        );

        debug!("\n\n COMPACTION \n {:?} \n\n", &self);
        Ok(())
    }

    /// Check if the current log can be compacted
    pub fn compaction_check(&mut self) -> Result<()> {
        debug!("COMPACTION!!!!!!!!!");
        if self.log_writer.is_none() {
            self.log_writer_init()?;
        }
        let writer_len = self.log_writer.as_mut().unwrap().metadata()?.len();
        if writer_len >= MAX_FILE_SIZE {
            debug!("starting compaction, log len = {writer_len}");
            self.compaction_manager()?
        } else {
            debug!("NOT starting compaction, log len = {writer_len}");
        }

        Ok(())
    }
}
