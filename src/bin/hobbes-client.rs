use clap::{Arg, Command};
// use kvs::client;
use kvs::engine::{KvStore, KvsError, Result};
use std::path::Path;

const DB_PATH: &str = "./";

fn main() -> Result<()> {
    let cmd = cli().get_matches();

    match cmd.subcommand() {
        Some(("get", sub_matches)) => {
            let key = sub_matches
                .get_one::<String>("get")
                .ok_or_else(|| KvsError::CliError(String::from("Unable to parse arguments")))?;

            let mut kv = KvStore::open(Path::new(DB_PATH))?;
            if let Some(val) = kv.get(key.clone())? {
                println!("{val}");
            } else {
                println!("Key not found");
            }
        }

        Some(("set", sub_matches)) => {
            let args: Vec<&String> = sub_matches
                .get_many::<String>("set")
                .into_iter()
                .flatten()
                .collect();

            let mut kv = KvStore::open(Path::new(DB_PATH))?;
            kv.set(args[0].clone(), args[1].clone())?;
        }

        Some(("rm", sub_matches)) => {
            let key = sub_matches
                .get_one::<String>("rm")
                .ok_or_else(|| KvsError::CliError(String::from("Unable to parse arguments")))?;

            let mut kv = KvStore::open(Path::new(DB_PATH))?;
            match kv.remove(key.clone()) {
                Ok(_) => {}
                Err(err) => match err {
                    KvsError::KeyNotFoundError => {
                        println!("Key not found");
                        std::process::exit(1);
                        // return Err(err);
                    }
                    _ => return Err(err),
                },
            };
        }
        _ => eprintln!("Invalid command"),
    }

    Ok(())
}

fn cli() -> Command {
    Command::new("kvs")
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
