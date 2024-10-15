# Hobbes

A [Bitcask](https://github.com/basho/bitcask/blob/develop/doc/bitcask-intro.pdf)-like
log-structured key-value store with an in-memory index, written in Rust

Link to the [blog](https://sudhir.live/posts/hobbes-log/)

## Usage

- Clone and install the project

```sh
git clone https://github.com/anirudhsudhir/hobbes.git
cargo install --path .
```

- Start the server

```txt
hobbes-server -h
Usage: hobbes-server [OPTIONS]

Options:
      --addr <addr>      set the server endpoint [default: 127.0.0.1:4000]
      --engine <engine>  set the storage engine [default: kvs]
  -h, --help             Print help
  -V, --version          Print version


hobbes-server
```

- Use the client to issue commands to the server

```txt
hobbes -h
A Bitcask-like log structured key-value store written in Rust

Usage: hobbes <COMMAND>

Commands:
  get   return the value associated with a key
  set   store a key-value pair
  rm    delete a key-value pair from the store
  help  Print this message or the help of the given subcommand(s)

Options:
  -h, --help     Print help
  -V, --version  Print version


hobbes set foo bar
hobbes get foo
hobbes rm foo
```

## Features

- Single mutable and multiple immutable logs: The store uses the Bitcask architecture. At any instance, the storage directory contains a mutable write-ahead log as well as several immutable logs
- Log Compaction: The store compacts logs when the filesize hits a certain threshold for efficient disk utilisation

## Client-server architecture

The key-value store is a server that listens for commands on the specified address. You may use a tool such as netcat instead of the hobbes client to send commands

```sh
echo "GET\r\nfoo\r\n" | nc localhost 4000
echo "SET\r\n<key>\r\n<val>\r\n" | nc <addr> <port>
echo "RM\r\n<key>\r\n" | nc <addr> <port>
```

The command and arguments are separated by a carriage return line feed (CRLF)(`\r\n`)
