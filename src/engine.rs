use tracing::{debug, error, info, warn};

use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::net::TcpListener;
use std::path::Path;

use super::{KvsError, Result};

pub mod hobbes;
pub mod sled_engine;

const DB_PARENT_PATH: &str = "";
const HOBBES_DB_PATH: &str = "hobbes-store/logs";
const HOBBES_COMPACTED_LOGS_PATH: &str = "hobbes-store/compacted-logs";
const SLED_DB_PATH: &str = "sled-store";

pub trait Engine {
    fn set(&mut self, key: String, value: String) -> Result<()>;
    fn get(&mut self, key: String) -> Result<Option<String>>;
    fn remove(&mut self, key: String) -> Result<()>;
}

pub fn start_server(addr: &str, engine: &str) -> Result<()> {
    let mut store: Box<dyn Engine> = match engine {
        "hobbes" => Box::new(hobbes::HobbesEngine::open(Path::new(&DB_PARENT_PATH))?),
        "sled" => Box::new(sled_engine::SledEngine::open(Path::new(&DB_PARENT_PATH))?),
        _ => Err(KvsError::CliError(String::from("invalid engine")))?,
    };
    let listener = TcpListener::bind(addr)?;

    for stream in listener.incoming() {
        let tcp_stream = stream?;
        let mut reader = BufReader::new(&tcp_stream);

        info!("==============================================");
        info!(client_addr = %tcp_stream.peer_addr()?, msg = "client connected");

        // Extracting the command length from the client request
        let mut cmd_prefix = String::new();
        reader.read_line(&mut cmd_prefix)?;
        let cmd_prefix_str = match cmd_prefix.strip_suffix("\r\n") {
            Some(val) => val,
            None => {
                error!("network command prefix not appended with \r\n, command = {cmd_prefix}");
                continue;
            }
        };

        debug!(
            server_addr = addr,
            client_addr = %tcp_stream.peer_addr()?,
            cmd_prefix = cmd_prefix,
            cmd_prefix_stripped = cmd_prefix_str,
            "Extracted command length from client request"
        );
        let cmd_len = match cmd_prefix_str.parse::<usize>() {
            Ok(val) => val,
            Err(err) => {
                error!(err = %err, "failed to parse the command length");
                continue;
            }
        };

        // Reading the command from the server
        let mut cmd_bytes = vec![0u8; cmd_len];
        reader.read_exact(&mut cmd_bytes)?;
        let cmd_str = match String::from_utf8(cmd_bytes.clone()) {
            Ok(val) => val,
            Err(err) => {
                error!(
                    err = %err,
                    "failed to parse command from client, command_bytes = {:?}", cmd_bytes
                );
                continue;
            }
        };

        debug!(
            server_addr = addr,
            client_addr = %tcp_stream.peer_addr()?,
            request = cmd_str,
            "Read command from client request"
        );

        let mut msg = cmd_str.split("\r\n");
        let cmd = msg.next().ok_or(KvsError::CliError(String::from(
            "Missing command in request",
        )))?;

        let mut resp = String::from("Success");
        match cmd {
            "GET" => {
                resp = handle_get(&mut store, msg)?;
            }
            "SET" => {
                handle_set(&mut store, msg)?;
            }
            "RM" => {
                resp = handle_rm(&mut store, msg)?;
            }
            _ => {
                error!(cmd = cmd, "Invalid command");
                resp = String::from("Invalid command");
            }
        }

        let mut writer = BufWriter::new(&tcp_stream);
        debug!(bytes = resp.len(), msg = "server response");
        writer.write_all(resp.as_bytes())?;
        writer.flush()?;

        debug!(cmd = cmd, response = resp, "Sent response to client");
    }

    Ok(())
}

fn handle_get<'a>(
    store: &mut Box<dyn Engine>,
    mut msg: impl Iterator<Item = &'a str>,
) -> Result<String> {
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

fn handle_set<'a>(
    store: &mut Box<dyn Engine>,
    mut msg: impl Iterator<Item = &'a str>,
) -> Result<()> {
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

fn handle_rm<'a>(
    store: &mut Box<dyn Engine>,
    mut msg: impl Iterator<Item = &'a str>,
) -> Result<String> {
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
