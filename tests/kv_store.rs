use anyhow::Result;

use hobbes_kv::engine::hobbes::HobbesEngine;
use tempfile::TempDir;
use walkdir::WalkDir;

use hobbes_kv::engine::Engine;

// Should get previously stored value
#[test]
fn get_stored_value() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut store = HobbesEngine::open(temp_dir.path())?;

    store.set("key1".to_owned(), "value1".to_owned())?;
    store.set("key2".to_owned(), "value2".to_owned())?;

    assert_eq!(store.get("key1".to_owned())?, Some("value1".to_owned()));
    assert_eq!(store.get("key2".to_owned())?, Some("value2".to_owned()));

    // Open from disk again and check persistent data
    drop(store);
    let mut store = HobbesEngine::open(temp_dir.path())?;
    assert_eq!(store.get("key1".to_owned())?, Some("value1".to_owned()));
    assert_eq!(store.get("key2".to_owned())?, Some("value2".to_owned()));

    Ok(())
}

// Should overwrite existent value
#[test]
fn overwrite_value() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut store = HobbesEngine::open(temp_dir.path())?;

    store.set("key1".to_owned(), "value1".to_owned())?;
    assert_eq!(store.get("key1".to_owned())?, Some("value1".to_owned()));
    store.set("key1".to_owned(), "value2".to_owned())?;
    assert_eq!(store.get("key1".to_owned())?, Some("value2".to_owned()));

    // Open from disk again and check persistent data
    drop(store);
    let mut store = HobbesEngine::open(temp_dir.path())?;
    assert_eq!(store.get("key1".to_owned())?, Some("value2".to_owned()));
    store.set("key1".to_owned(), "value3".to_owned())?;
    assert_eq!(store.get("key1".to_owned())?, Some("value3".to_owned()));

    Ok(())
}

// Should get `None` when getting a non-existent key
#[test]
fn get_non_existent_value() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut store = HobbesEngine::open(temp_dir.path())?;

    store.set("key1".to_owned(), "value1".to_owned())?;
    assert_eq!(store.get("key2".to_owned())?, None);

    // Open from disk again and check persistent data
    drop(store);
    let mut store = HobbesEngine::open(temp_dir.path())?;
    assert_eq!(store.get("key2".to_owned())?, None);

    Ok(())
}

#[test]
fn remove_non_existent_key() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut store = HobbesEngine::open(temp_dir.path())?;
    assert!(store.remove("key1".to_owned()).is_err());
    Ok(())
}

#[test]
fn remove_key() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut store = HobbesEngine::open(temp_dir.path())?;
    store.set("key1".to_owned(), "value1".to_owned())?;
    assert!(store.remove("key1".to_owned()).is_ok());
    assert_eq!(store.get("key1".to_owned())?, None);
    Ok(())
}

// Insert data until total size of the directory decreases.
// Test data correctness after compaction.
#[test]
fn compaction() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut store = HobbesEngine::open(temp_dir.path())?;
    let count = 1000;

    let dir_size = || {
        // let entries = WalkDir::new(temp_dir.path()).into_iter();
        let entries = WalkDir::new(&temp_dir).into_iter();
        let len: walkdir::Result<u64> = entries
            .map(|res| {
                res.and_then(|entry| entry.metadata())
                    .map(|metadata| metadata.len())
            })
            .sum();
        len.expect("fail to get directory size")
    };

    let mut current_size = dir_size();
    for iter in 0..count {
        for key_id in 0..count {
            let key = format!("key{}", key_id);
            let value = format!("{}", iter);
            store.set(key, value)?;
        }

        let new_size = dir_size();
        if new_size > current_size {
            current_size = new_size;
            continue;
        }
        // Compaction triggered

        drop(store);
        // reopen and check content
        let mut store = HobbesEngine::open(temp_dir.path())?;
        for key_id in 0..count {
            let key = format!("key{}", key_id);
            let store_val = store.get(key.clone())?;
            if store_val != Some(format!("{}", iter)) {
                panic!(
                    "key = {}, store_val = {:?}, expected val = {:?}",
                    &key,
                    store_val,
                    Some(format!("{}", iter))
                );
            }
        }
        return Ok(());
    }

    panic!("No compaction detected");
}
