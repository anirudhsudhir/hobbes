use sled;
use tracing::error;

use std::path::Path;

use super::{Engine, KvsError, Result, HOBBES_LOGS_PATH, SLED_DB_PATH};

#[derive(Clone)]
pub struct SledEngine {
    db: sled::Db,
}

impl SledEngine {
    /// Open an instance of SledEngine at the specified directory
    pub fn open(logs_dir_arg: &Path) -> Result<SledEngine> {
        // Check if a sled-store already exists
        let hobbes_store_dir = logs_dir_arg.join(HOBBES_LOGS_PATH);
        if Path::is_dir(&hobbes_store_dir) {
            Err(KvsError::CliError(String::from(
                "hobbes storage engine used previously, using the sled engine is an invalid operation",
            )))?
        }

        let logs_dir = logs_dir_arg.join(SLED_DB_PATH);
        let db = sled::open(logs_dir)?;
        Ok(SledEngine { db })
    }
}

impl Engine for SledEngine {
    fn get(&self, key: String) -> Result<Option<String>> {
        match self.db.get(key)? {
            Some(val) => match String::from_utf8(val.to_vec()) {
                Ok(val) => Ok(Some(val)),
                Err(err) => {
                    error!(err=%err, "failed to parse value retrieved from sled engine");
                    Ok(None)
                }
            },
            None => Ok(None),
        }
    }

    fn set(&self, key: String, value: String) -> Result<()> {
        let set_ret = self.db.insert(key, value.as_bytes());
        match set_ret {
            Ok(_) => {
                self.db.flush()?;
                Ok(())
            }
            Err(err) => Err(KvsError::SledDbError(err)),
        }
    }

    fn remove(&self, key: String) -> Result<()> {
        let rm_ret = self.db.remove(key.as_bytes());
        match rm_ret {
            Ok(opt) => match opt {
                Some(_) => {
                    self.db.flush()?;
                    Ok(())
                }
                None => Err(KvsError::KeyNotFoundError),
            },
            Err(err) => Err(KvsError::SledDbError(err)),
        }
    }
}
