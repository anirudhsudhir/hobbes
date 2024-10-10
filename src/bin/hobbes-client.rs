use clap::{Arg, Command};
use tracing::info;

use std::io::Write;
use std::net::TcpStream;

use hobbes::{KvsError, Result};

const SERVER_ADDR: &str = "localhost:4000";

fn main() -> Result<()> {
    let cmd = cli().get_matches();

    match cmd.subcommand() {
        Some(("get", sub_matches)) => {
            let key = sub_matches
                .get_one::<String>("get")
                .ok_or_else(|| KvsError::CliError(String::from("Unable to parse arguments")))?;

            let cmd = String::from("GET\r\n") + key + "\r\n";
            send_cmd(cmd)?;
        }

        Some(("set", sub_matches)) => {
            let mut args = sub_matches.get_many::<String>("set").into_iter().flatten();
            let key = args.next().ok_or(KvsError::CliError(String::from(
                "Missing key in SET command",
            )))?;
            let val = args.next().ok_or(KvsError::CliError(String::from(
                "Missing value in SET command",
            )))?;

            let cmd = String::from("SET\r\n") + key + "\r\n" + val + "\r\n";
            send_cmd(cmd)?;
        }

        Some(("rm", sub_matches)) => {
            let key = sub_matches
                .get_one::<String>("rm")
                .ok_or_else(|| KvsError::CliError(String::from("Unable to parse arguments")))?;
            let cmd = String::from("RM\r\n") + key + "\r\n";
            send_cmd(cmd)?;
        }
        _ => eprintln!("Invalid command"),
    }

    Ok(())
}

fn cli() -> Command {
    Command::new("hobbes-client")
        .name(env!("CARGO_BIN_NAME"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .version(env!("CARGO_PKG_VERSION"))
        .subcommand_required(true)
        .subcommand(
            Command::new("get")
                .about("return the value associated with a key")
                .arg_required_else_help(true)
                .arg(
                    Arg::new("get")
                        .help("key whose value is to be retrieved")
                        .value_name("KEY")
                        .num_args(1),
                ),
        )
        .subcommand(
            Command::new("set")
                .about("store a key-value pair")
                .arg_required_else_help(true)
                .arg(
                    Arg::new("set")
                        .help("key-value pair to be stored")
                        .value_names(["KEY", "VALUE"])
                        .num_args(2),
                ),
        )
        .subcommand(
            Command::new("rm")
                .about("delete a key-value pair from the store")
                .arg_required_else_help(true)
                .arg(
                    Arg::new("rm")
                        .help("key-value pair to be deleted from the store")
                        .value_name("KEY")
                        .num_args(1),
                ),
        )
}

fn send_cmd(cmd: String) -> Result<()> {
    let mut conn = TcpStream::connect(SERVER_ADDR)?;
    conn.write_all(cmd.as_bytes())?;
    conn.flush()?;

    info!(
        cmd = cmd,
        server_addr = SERVER_ADDR,
        "Sent command over the network"
    );

    Ok(())
}
