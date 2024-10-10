use tracing::{error, info, warn};

use std::io::Read;
use std::net::TcpListener;
use std::path::Path;

use super::{KvsError, Result};

pub mod storage;

const DB_PATH: &str = "./";

pub fn start_server(addr: &str) -> Result<()> {
    info!("starting hobbes server");

    let listener = TcpListener::bind(addr)?;

    for stream in listener.incoming() {
        let mut data = String::new();
        match stream {
            Ok(mut stream) => {
                info!(addr = %stream.peer_addr()?, msg = "client connected");
                stream.read_to_string(&mut data)?;
            }
            Err(err) => {
                error!("cli error: {}", err);
            }
        }

        let mut msg = data.split("\r\n");
        let cmd = msg.next().ok_or(KvsError::CliError(String::from(
            "Missing command in request",
        )))?;
        match cmd {
            "GET" => {
                handle_get(msg)?;
            }
            "SET" => {
                handle_set(msg)?;
            }
            "RM" => {
                handle_rm(msg)?;
            }
            _ => {
                error!(cmd = cmd, "Invalid command");
            }
        }
    }

    Ok(())
}

fn handle_get<'a>(mut msg: impl Iterator<Item = &'a str>) -> Result<()> {
    let key = msg
        .next()
        .ok_or(KvsError::CliError(String::from(
            "Missing key in GET command",
        )))?
        .trim();
    info!(cmd = "GET", key = key, "Received command");

    let mut kv = storage::KvStore::open(Path::new(DB_PATH))?;
    if let Some(val) = kv.get(key.to_string())? {
        info!(cmd = "GET", key = key, val = val, "Successful query");
        // println!("{val}");
    } else {
        warn!(cmd = "GET", key = key, "Key not found");
        // println!("Key not found");
    }

    Ok(())
}

fn handle_set<'a>(mut msg: impl Iterator<Item = &'a str>) -> Result<()> {
    let key = msg
        .next()
        .ok_or(KvsError::CliError(String::from(
            "Missing key in SET command",
        )))?
        .trim();
    let val = msg
        .next()
        .ok_or(KvsError::CliError(String::from(
            "Missing value in SET command",
        )))?
        .trim();
    info!(cmd = "SET", key = key, val = val, "Received command");

    let mut kv = storage::KvStore::open(Path::new(DB_PATH))?;
    kv.set(key.to_string(), val.to_string())?;
    info!(cmd = "SET", key = key, val = val, "Successful query");

    Ok(())
}

fn handle_rm<'a>(mut msg: impl Iterator<Item = &'a str>) -> Result<()> {
    let key = msg
        .next()
        .ok_or(KvsError::CliError(String::from(
            "Missing key in RM command",
        )))?
        .trim();
    info!(cmd = "RM", key = key, "Received command");

    let mut kv = storage::KvStore::open(Path::new(DB_PATH))?;
    match kv.remove(key.to_string()) {
        Ok(_) => {}
        Err(err) => match err {
            KvsError::KeyNotFoundError => {
                println!("Key not found");
                std::process::exit(1);
            }
            _ => return Err(err),
        },
    };
    info!(cmd = "RM", key = key, "Successful query");

    Ok(())
}
