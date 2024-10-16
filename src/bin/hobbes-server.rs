use clap::{Arg, Command};
use tracing::info;
use tracing_subscriber::fmt::time::LocalTime;
use tracing_subscriber::FmtSubscriber;

use std::io;

use hobbes::engine;
use hobbes::{KvsError, Result};

fn main() -> Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(tracing::Level::TRACE)
        .with_timer(LocalTime::rfc_3339())
        .with_target(true)
        .with_writer(io::stderr)
        .finish();

    tracing::subscriber::set_global_default(subscriber)?;

    // TODO: add checks for args
    let command = Command::new("hobbes-server")
        .version(env!("CARGO_PKG_VERSION"))
        .arg(
            Arg::new("addr")
                .help("set the server endpoint")
                .long("addr")
                .default_value("127.0.0.1:4000"),
        )
        .arg(
            Arg::new("engine")
                .help("set the storage engine")
                .long("engine")
                .default_value("kvs"),
        )
        .get_matches();

    let addr = command
        .get_one::<String>("addr")
        .ok_or_else(|| KvsError::CliError(String::from("failed to parse argument \"addr\"")))?;
    let engine = command
        .get_one::<String>("engine")
        .ok_or_else(|| KvsError::CliError(String::from("failed to parse argument \"engine\"")))?;

    info!("version: {}", env!("CARGO_PKG_VERSION"));
    info!(addr, engine);

    engine::start_server(addr)?;

    Ok(())
}
