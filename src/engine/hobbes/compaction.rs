use rmp_serde::{self, decode};

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Seek, Write};
use std::path::PathBuf;

use super::{
    serialize_command, HobbesEngine, LogEntry, Result, ValueMetadata, LOG_EXTENSION, TOMBSTONE,
};

const MAX_FILE_SIZE: u64 = 10000;

impl HobbesEngine {
    fn compaction_manager(&mut self) -> Result<()> {
        // Stopping writes to the current log and creating a new log
        let compacted_log_id = self.current_log_id;
        self.current_log_id += 1;
        let new_log_path = self.logs_dir.join(PathBuf::from(
            self.current_log_id.to_string() + LOG_EXTENSION,
        ));
        self.log_writer = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&new_log_path)?;
        self.log_readers.insert(
            self.current_log_id,
            BufReader::new(File::open(&new_log_path)?),
        );

        // Map storing the latest key-value pairs
        let mut compacted_store_map = HashMap::new();
        // The updated in-memory index
        let mut updated_index = HashMap::new();

        let stale_log_path = self
            .logs_dir
            .join(PathBuf::from(compacted_log_id.to_string() + LOG_EXTENSION));
        let mut log_reader = BufReader::new(File::open(&stale_log_path)?);

        // Replaying the commands of the stale log
        while let Ok(decode_cmd) = decode::from_read(&mut log_reader) {
            let cmd: LogEntry = decode_cmd;
            match cmd.val.as_str() {
                TOMBSTONE => compacted_store_map.remove(&cmd.key),
                _ => compacted_store_map.insert(cmd.key, cmd.val),
            };
        }

        // Creating the compacted log
        let compacted_log_path = self.logs_dir.join(PathBuf::from(
            compacted_log_id.to_string() + "_compacted" + LOG_EXTENSION,
        ));
        let compacted_log_file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&compacted_log_path)?;
        let mut compacted_log_writer = BufWriter::new(compacted_log_file);

        // Persisting compacted logs and updating the index
        let mut offset: u64 = 0;
        for (key, val) in compacted_store_map.into_iter() {
            let cmd = serialize_command(&LogEntry {
                key: key.clone(),
                val,
            })?;

            compacted_log_writer.write_all(&cmd)?;
            updated_index.insert(
                key,
                ValueMetadata {
                    log_pointer: offset,
                    log_id: compacted_log_id,
                },
            );
            offset = compacted_log_writer.stream_position()?;
        }

        // Updating KvStore
        fs::rename(&compacted_log_path, &stale_log_path)?;
        self.mem_index = updated_index;
        self.log_readers.insert(
            compacted_log_id,
            BufReader::new(File::open(&stale_log_path)?),
        );

        Ok(())
    }

    /// Check if the current log can be compacted
    pub fn compaction_check(&mut self) -> Result<()> {
        if self.log_writer.stream_position()? >= MAX_FILE_SIZE {
            self.compaction_manager()?
        }
        Ok(())
    }
}
