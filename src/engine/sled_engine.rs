use sled;

use std::path::Path;

use super::{Engine, KvsError, Result, HOBBES_DB_PATH, SLED_DB_PATH};

pub struct SledEngine {
    db: sled::Db,
}

impl SledEngine {
    /// Open an instance of SledEngine at the specified directory
    pub fn open(logs_dir_arg: &Path) -> Result<SledEngine> {
        // Check if a sled-store already exists
        let hobbes_store_dir = logs_dir_arg.join(HOBBES_DB_PATH);
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
    fn get(&mut self, key: String) -> Result<Option<String>> {
        match self.db.get(key)? {
            Some(val) => Ok(Some(String::from_utf8(val.to_vec())?)),
            None => Ok(None),
        }
    }

    fn set(&mut self, key: String, value: String) -> Result<()> {
        let set_ret = self.db.insert(key.as_bytes(), value.as_bytes());
        self.db.flush()?;
        match set_ret {
            Ok(_) => Ok(()),
            Err(err) => Err(KvsError::SledDbError(err)),
        }
    }

    fn remove(&mut self, key: String) -> Result<()> {
        let rm_ret = self.db.remove(key.as_bytes());
        self.db.flush()?;
        match rm_ret {
            Ok(opt) => match opt {
                Some(_) => Ok(()),
                None => Err(KvsError::KeyNotFoundError),
            },
            Err(err) => Err(KvsError::SledDbError(err)),
        }
    }
}