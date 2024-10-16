use clap::{Arg, Command};
use tracing::debug;
use tracing_subscriber::fmt::time::LocalTime;
use tracing_subscriber::FmtSubscriber;

use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::net::TcpStream;
use std::process;

use hobbes::{KvsError, Result};

fn main() -> Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_timer(LocalTime::rfc_3339())
        .with_target(true)
        .with_test_writer()
        .with_writer(io::stdout)
        .finish();

    tracing::subscriber::set_global_default(subscriber)?;

    let cmd = cli().get_matches();

    let addr = cmd
        .get_one::<String>("addr")
        .ok_or_else(|| KvsError::CliError(String::from("failed to parse argument \"addr\"")))?
        .to_string();

    match cmd.subcommand() {
        Some(("get", sub_matches)) => {
            let key = sub_matches
                .get_one::<String>("get")
                .ok_or_else(|| KvsError::CliError(String::from("Unable to parse arguments")))?;

            let cmd = String::from("GET\r") + key + "\r\n";
            let resp = send_cmd(cmd, addr)?;
            match resp.as_str() {
                "Key not found" => println!("{resp}"),
                resp => println!("{resp}"),
            }
        }

        Some(("set", sub_matches)) => {
            let mut args = sub_matches.get_many::<String>("set").into_iter().flatten();
            let key = args.next().ok_or(KvsError::CliError(String::from(
                "Missing key in SET command",
            )))?;
            let val = args.next().ok_or(KvsError::CliError(String::from(
                "Missing value in SET command",
            )))?;

            let cmd = String::from("SET\r") + key + "\r" + val + "\r\n";
            send_cmd(cmd, addr)?;
        }

        Some(("rm", sub_matches)) => {
            let key = sub_matches
                .get_one::<String>("rm")
                .ok_or_else(|| KvsError::CliError(String::from("Unable to parse arguments")))?;
            let cmd = String::from("RM\r") + key + "\r\n";
            let resp = send_cmd(cmd, addr)?;
            if resp == "Key not found" {
                eprintln!("{resp}");
                process::exit(1);
            }
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
        .arg(
            Arg::new("addr")
                .help("set the endpoint to connect to")
                .long("addr")
                .default_value("127.0.0.1:4000"),
        )
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
                )
                .arg(
                    Arg::new("addr")
                        .help("set the endpoint to connect to")
                        .long("addr")
                        .default_value("127.0.0.1:4000"),
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
                )
                .arg(
                    Arg::new("addr")
                        .help("set the endpoint to connect to")
                        .long("addr")
                        .default_value("127.0.0.1:4000"),
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

fn send_cmd(cmd: String, addr: String) -> Result<String> {
    let stream = TcpStream::connect(&addr)?;
    let mut writer = BufWriter::new(&stream);
    writer.write_all(cmd.as_bytes())?;
    writer.flush()?;
    debug!(cmd = cmd, server_addr = addr, "Sent command to the server");

    let mut resp = String::new();
    let mut reader = BufReader::new(&stream);
    reader.read_line(&mut resp)?;

    debug!(
        cmd = cmd,
        server_addr = addr,
        response = resp,
        "Recieved response from server"
    );

    Ok(resp)
}
