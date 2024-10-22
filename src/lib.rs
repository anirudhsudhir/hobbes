// #![deny(missing_docs)]

//! A Bitcask-like log-structured key-value store with an in-memory index

use rmp_serde::{decode, encode};
use tracing::subscriber;

use std::{fmt, io, num, path};

pub mod engine;

/// Error Types for the store
#[derive(Debug)]
pub enum KvsError {
    /// Indicates I/O errors
    IoError(io::Error),
    /// Indicates errors while serializing commands to be stored in the on-disk log
    SerializationError(encode::Error),
    /// Indicates errors while deserializing commands from the on-disk log
    DeserializationError(decode::Error),
    /// Indicates absence of key in the Store
    KeyNotFoundError,
    /// Indicates CLI errors
    CliError(String),
    /// Indicates error while stripping filepath prefixes
    StripPrefixError(path::StripPrefixError),
    /// Indicates error while parsing string to int
    ParseIntError(num::ParseIntError),
    /// Indicates an missing log reader
    LogReaderNotFoundError(String),
    /// Indicates an error while setting the global default tracing subscriber for structured
    /// logging
    SetGlobalDefaultError(subscriber::SetGlobalDefaultError),
    /// Indicates errors arising from the sled Db type
    SledDbError(sled::Error),
}

/// Result type for the store
pub type Result<T> = std::result::Result<T, KvsError>;

pub struct KvsEngine {}

impl fmt::Display for KvsError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            KvsError::IoError(ref err) => write!(f, "IO error: {}", err),
            KvsError::SerializationError(ref err) => write!(f, "Serialization error: {}", err),
            KvsError::DeserializationError(ref err) => write!(f, "Deserialization error: {}", err),
            KvsError::KeyNotFoundError => write!(f, "Key not found",),
            KvsError::CliError(ref err) => write!(f, "CLI Error: {}", err),
            KvsError::StripPrefixError(ref err) => write!(f, "Strip Prefix Error: {}", err),
            KvsError::ParseIntError(ref err) => write!(f, "Parse Int Error: {}", err),
            KvsError::LogReaderNotFoundError(ref err) => {
                write!(f, "Log Reader Not Found Error: {}", err)
            }
            KvsError::SetGlobalDefaultError(ref err) => {
                write!(f, "Set Global Default Error: {}", err)
            }
            KvsError::SledDbError(ref err) => write!(f, "Sled Engine Error: {}", err),
        }
    }
}

impl From<std::io::Error> for KvsError {
    fn from(value: std::io::Error) -> Self {
        KvsError::IoError(value)
    }
}

impl From<encode::Error> for KvsError {
    fn from(value: encode::Error) -> Self {
        KvsError::SerializationError(value)
    }
}

impl From<decode::Error> for KvsError {
    fn from(value: decode::Error) -> Self {
        KvsError::DeserializationError(value)
    }
}

impl From<path::StripPrefixError> for KvsError {
    fn from(value: path::StripPrefixError) -> Self {
        KvsError::StripPrefixError(value)
    }
}

impl From<num::ParseIntError> for KvsError {
    fn from(value: num::ParseIntError) -> Self {
        KvsError::ParseIntError(value)
    }
}

impl From<subscriber::SetGlobalDefaultError> for KvsError {
    fn from(value: subscriber::SetGlobalDefaultError) -> Self {
        KvsError::SetGlobalDefaultError(value)
    }
}

impl From<sled::Error> for KvsError {
    fn from(value: sled::Error) -> Self {
        KvsError::SledDbError(value)
    }
}
