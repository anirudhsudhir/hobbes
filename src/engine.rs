use tracing::{error, info, warn};

use std::io::{BufRead, BufReader, BufWriter, Write};
use std::net::TcpListener;
use std::path::Path;

use super::{KvsError, Result};

pub mod hobbes;
pub mod sled_engine;

const DB_PARENT_PATH: &str = "./";
const HOBBES_DB_PATH: &str = "./hobbes-store";
const SLED_DB_PATH: &str = "./sled-store";

pub trait Engine {
    fn set(&mut self, key: String, value: String) -> Result<()>;
    fn get(&mut self, key: String) -> Result<Option<String>>;
    fn remove(&mut self, key: String) -> Result<()>;
}

pub fn start_server(addr: &str, engine: &str) -> Result<()> {
    info!(server_addr = addr, "starting hobbes server");

    let mut store: Box<dyn Engine> = match engine {
        "hobbes" => Box::new(hobbes::HobbesEngine::open(Path::new(&DB_PARENT_PATH))?),
        "sled" => Box::new(sled_engine::SledEngine::open(Path::new(&DB_PARENT_PATH))?),
        _ => Err(KvsError::CliError(String::from("invalid engine")))?,
    };
    let listener = TcpListener::bind(addr)?;

    for stream in listener.incoming() {
        let tcp_stream = stream?;
        let mut reader = BufReader::new(&tcp_stream);
        info!(client_addr = %tcp_stream.peer_addr()?, msg = "client connected");

        let mut data = String::new();
        reader.read_line(&mut data)?;

        info!(
            server_addr = addr,
            client_addr = %tcp_stream.peer_addr()?,
            request = data,
            "Recieved command from client"
        );

        let mut msg = data.split("\r");
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
        writer.write_all(resp.as_bytes())?;
        writer.flush()?;

        info!(cmd = cmd, response = resp, "Sent response to client");
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
