use clap::{Arg, Command};
use kvs::Result;

fn main() -> Result<()> {
    let cmd = Command::new("kvs")
        .name(env!("CARGO_BIN_NAME"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .version(env!("CARGO_PKG_VERSION"))
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
        .get_matches();

    // match cmd.subcommand() {
    // Some((_, _)) => Err("unimplemented"),
    // _ => Err("no subcommands or arguments specified"),
    // }

    Ok(())
}
