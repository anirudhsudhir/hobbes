use bitcask::BitcaskEngine;
use sled_engine::SledEngine;
use tracing::{debug, error, info, warn};

use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;

use crate::thread_pool::{NaiveThreadPool, ThreadPool};

use super::{KvsError, Result};

pub mod bitcask;
pub mod sled_engine;

const DB_PARENT_PATH: &str = "";
const HOBBES_LOGS_PATH: &str = "hobbes-store/logs";
const HOBBES_DB_PATH: &str = "hobbes-store/";
const HOBBES_COMPACTED_LOGS_SUBPATH: &str = "compacted-logs/";
const SLED_DB_PATH: &str = "sled-store";

pub trait Engine: Clone + Send + 'static {
    fn set(&self, key: String, value: String) -> Result<()>;
    fn get(&self, key: String) -> Result<Option<String>>;
    fn remove(&self, key: String) -> Result<()>;
}

#[derive(Clone)]
enum EngineType {
    Bitcask(BitcaskEngine),
    Sled(SledEngine),
}

impl Engine for EngineType {
    fn set(&self, key: String, value: String) -> Result<()> {
        match self {
            EngineType::Bitcask(bitcask_engine) => bitcask_engine.set(key, value),
            EngineType::Sled(sled_engine) => sled_engine.set(key, value),
        }
    }
    fn get(&self, key: String) -> Result<Option<String>> {
        match self {
            EngineType::Bitcask(bitcask_engine) => bitcask_engine.get(key),
            EngineType::Sled(sled_engine) => sled_engine.get(key),
        }
    }
    fn remove(&self, key: String) -> Result<()> {
        match self {
            EngineType::Bitcask(bitcask_engine) => bitcask_engine.remove(key),
            EngineType::Sled(sled_engine) => sled_engine.remove(key),
        }
    }
}

pub fn start_server(addr: &str, engine: &str) -> Result<()> {
    let store: EngineType = match engine {
        "hobbes" => EngineType::Bitcask(bitcask::BitcaskEngine::open(Path::new(&DB_PARENT_PATH))?),
        "sled" => EngineType::Sled(sled_engine::SledEngine::open(Path::new(&DB_PARENT_PATH))?),
        _ => Err(KvsError::CliError(String::from("invalid engine")))?,
    };

    // The count is an unused variable, this naive threadpool implementation is for learning
    // purposes
    let naive_thread_pool = NaiveThreadPool::new(0)?;

    let listener = TcpListener::bind(addr)?;

    for tcp_stream in listener.incoming().flatten() {
        let addr_clone = addr.to_owned();
        let store_clone = store.clone();
        naive_thread_pool.spawn(move || req_handler(store_clone, tcp_stream, addr_clone))
    }

    Ok(())
}

fn req_handler(store: EngineType, mut tcp_stream: TcpStream, addr: String) {
    let peer_addr = match tcp_stream.peer_addr() {
        Ok(addr) => addr,
        Err(e) => {
            error!("Error while reading the peer address from TCP stream -> {e}");
            return;
        }
    };

    let mut reader = BufReader::new(&mut tcp_stream);

    info!("==============================================");
    info!(client_addr = %peer_addr, msg = "client connected");

    // Extracting the command length from the client request
    let mut cmd_prefix = String::new();
    if let Err(e) = reader.read_line(&mut cmd_prefix) {
        error!("Error while reading line from TCP stream -> {e}");
        return;
    }
    let cmd_prefix_str = match cmd_prefix.strip_suffix("\r\n") {
        Some(val) => val,
        None => {
            error!("network command prefix not appended with \r\n, command = {cmd_prefix}");
            return;
        }
    };

    debug!(
        server_addr = addr,
        client_addr = %peer_addr,
        cmd_prefix = cmd_prefix,
        cmd_prefix_stripped = cmd_prefix_str,
        "Extracted command length from client request"
    );
    let cmd_len = match cmd_prefix_str.parse::<usize>() {
        Ok(val) => val,
        Err(err) => {
            error!(err = %err, "failed to parse the command length");
            return;
        }
    };

    // Reading the command from the server
    let mut cmd_bytes = vec![0u8; cmd_len];
    if let Err(e) = reader.read_exact(&mut cmd_bytes) {
        error!("Error while reading exact bytes from command -> {e}");
        return;
    }

    let cmd_str = match String::from_utf8(cmd_bytes.clone()) {
        Ok(val) => val,
        Err(err) => {
            error!(
                err = %err,
                "failed to parse command from client, command_bytes = {:?}", cmd_bytes
            );
            return;
        }
    };

    debug!(
        server_addr = addr,
        client_addr = %peer_addr,
        request = cmd_str,
        "Read command from client request"
    );

    let mut msg = cmd_str.split("\r\n");
    let cmd;
    if let Some(parsed_cmd) = msg.next() {
        cmd = parsed_cmd;
    } else {
        error!("Missing command in request");
        return;
    }

    let mut resp = String::from("Success");
    match cmd {
        "GET" => match handle_get(store, msg) {
            Ok(res) => resp = res,
            Err(e) => {
                error!("Failed to handle get command for request = {cmd_str}, error = {e}");
                return;
            }
        },
        "SET" => {
            if let Err(e) = handle_set(store, msg) {
                error!("Failed to handle set command for request = {cmd_str}, error = {e}");
                return;
            }
        }
        "RM" => match handle_rm(store, msg) {
            Ok(res) => resp = res,
            Err(e) => {
                error!("Failed to handle rm command for request = {cmd_str}, error = {e}");
                return;
            }
        },
        _ => {
            error!(cmd = cmd, "Invalid command");
            resp = String::from("Invalid command");
        }
    }

    let mut writer = BufWriter::new(&tcp_stream);
    debug!(bytes = resp.len(), msg = "server response");
    if let Err(e) = writer.write_all(resp.as_bytes()) {
        error!("Error while writing to response to client -> {e}");
        return;
    }

    if let Err(e) = writer.flush() {
        error!("Error while flushing to response to client -> {e}");
        return;
    }

    debug!(cmd = cmd, response = resp, "Sent response to client");
}

fn handle_get<'a>(store: EngineType, mut msg: impl Iterator<Item = &'a str>) -> Result<String> {
    let key = msg
        .next()
        .ok_or(KvsError::CliError(String::from(
            "Missing key in GET command",
        )))?
        .trim();
    info!(cmd = "GET", key = key, "Received command");

    if let Some(val) = store.get(key.to_string())? {
        info!(cmd = "GET", key = key, val = val, "Successful query");
        Ok(val)
    } else {
        warn!(cmd = "GET", key = key, "Key not found");
        Ok(String::from("Key not found"))
    }
}

fn handle_set<'a>(store: EngineType, mut msg: impl Iterator<Item = &'a str>) -> Result<()> {
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

    store.set(key.to_string(), val.to_string())?;
    info!(cmd = "SET", key = key, val = val, "Successful query");

    Ok(())
}

fn handle_rm<'a>(store: EngineType, mut msg: impl Iterator<Item = &'a str>) -> Result<String> {
    let key = msg
        .next()
        .ok_or(KvsError::CliError(String::from(
            "Missing key in RM command",
        )))?
        .trim();
    info!(cmd = "RM", key = key, "Received command");

    match store.remove(key.to_string()) {
        Ok(_) => {
            info!(cmd = "RM", key = key, "Successful query");
            Ok(String::from("Success"))
        }
        Err(err) => match err {
            KvsError::KeyNotFoundError => {
                info!(cmd = "RM", key = key, "Key not found");
                Ok(String::from("Key not found"))
            }
            _ => Err(err),
        },
    }
}
