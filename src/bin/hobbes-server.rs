use clap::{Arg, Command};
use tracing_subscriber::fmt::time;
use tracing_subscriber::FmtSubscriber;

use std::env;
use std::io;

use hobbes_kv::engine;
use hobbes_kv::{KvsError, Result};

fn main() -> Result<()> {
    let logging_level = match env::var("LOG_LEVEL") {
        Ok(level) => match level.as_str() {
            "TRACE" => tracing::Level::TRACE,
            "DEBUG" => tracing::Level::DEBUG,
            "INFO" => tracing::Level::INFO,
            "WARN" => tracing::Level::WARN,
            "ERROR" => tracing::Level::ERROR,
            _ => tracing::Level::INFO,
        },
        Err(_) => tracing::Level::INFO,
    };

    let subscriber = FmtSubscriber::builder()
        .with_max_level(logging_level)
        .with_timer(time::ChronoLocal::rfc_3339())
        .with_target(true)
        .with_writer(io::stderr)
        .finish();

    tracing::subscriber::set_global_default(subscriber)?;

    let command = Command::new("hobbes-server")
        .version(env!("CARGO_PKG_VERSION"))
        .arg(
            Arg::new("addr")
                .help("set the server endpoint")
                .long("addr")
                .default_value("127.0.0.1:4000")
                .num_args(1),
        )
        .arg(
            Arg::new("engine")
                .help("set the storage engine")
                .long("engine")
                .default_value("hobbes")
                .num_args(1)
                .value_parser(["hobbes", "sled"]),
        )
        .get_matches();

    let addr = command
        .get_one::<String>("addr")
        .ok_or_else(|| KvsError::CliError(String::from("failed to parse argument \"addr\"")))?;
    let engine = command
        .get_one::<String>("engine")
        .ok_or_else(|| KvsError::CliError(String::from("failed to parse argument \"engine\"")))?;

    println!(
        r"
    __          __    __
   / /_  ____  / /_  / /_  ___  _____
  / __ \/ __ \/ __ \/ __ \/ _ \/ ___/
 / / / / /_/ / /_/ / /_/ /  __(__  )
/_/ /_/\____/_.___/_.___/\___/____/

    "
    );
    println!("Using engine [{engine}] and serving at address {addr}");
    println!("Version [{}]", env!("CARGO_PKG_VERSION"));

    engine::start_server(addr, engine)?;

    Ok(())
}
