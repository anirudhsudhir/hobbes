// #![deny(missing_docs)]

//! A Bitcask-like log-structured key-value store with an in-memory index

use rmp_serde::{decode, encode};
use tracing::subscriber;

use std::{fmt, io, num, path};

pub mod engine;
pub mod thread_pool;

const RWLOCK_ERROR: &str = "Failed to lock RwLock";

type Job = Box<dyn FnOnce() + Send + 'static>;

/// Error Types for the store
#[derive(Debug)]
pub enum HobbesError {
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
    /// Indicates compaction errors
    CompactionError(String),
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
    /// Indicates errors arising during network communication between server and client
    NetworkError(String),
    /// Indicates errors while sending types over a channel
    ChannelSendError(String),
}

/// Result type for the store
pub type Result<T> = std::result::Result<T, HobbesError>;

impl fmt::Display for HobbesError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            HobbesError::IoError(ref err) => write!(f, "IO error: {}", err),
            HobbesError::SerializationError(ref err) => write!(f, "Serialization error: {}", err),
            HobbesError::DeserializationError(ref err) => {
                write!(f, "Deserialization error: {}", err)
            }
            HobbesError::KeyNotFoundError => write!(f, "Key not found",),
            HobbesError::CliError(ref err) => write!(f, "CLI Error: {}", err),
            HobbesError::CompactionError(ref err) => write!(f, "Compaction Error: {}", err),
            HobbesError::StripPrefixError(ref err) => write!(f, "Strip Prefix Error: {}", err),
            HobbesError::ParseIntError(ref err) => write!(f, "Parse Int Error: {}", err),
            HobbesError::LogReaderNotFoundError(ref err) => {
                write!(f, "Log Reader Not Found Error: {}", err)
            }
            HobbesError::SetGlobalDefaultError(ref err) => {
                write!(f, "Set Global Default Error: {}", err)
            }
            HobbesError::SledDbError(ref err) => write!(f, "Sled Engine Error: {}", err),
            HobbesError::NetworkError(ref err) => write!(f, "Network Error: {}", err),
            HobbesError::ChannelSendError(ref err) => write!(f, "Channel Send Error: {}", err),
        }
    }
}

impl From<std::io::Error> for HobbesError {
    fn from(value: std::io::Error) -> Self {
        HobbesError::IoError(value)
    }
}

impl From<encode::Error> for HobbesError {
    fn from(value: encode::Error) -> Self {
        HobbesError::SerializationError(value)
    }
}

impl From<decode::Error> for HobbesError {
    fn from(value: decode::Error) -> Self {
        HobbesError::DeserializationError(value)
    }
}

impl From<path::StripPrefixError> for HobbesError {
    fn from(value: path::StripPrefixError) -> Self {
        HobbesError::StripPrefixError(value)
    }
}

impl From<num::ParseIntError> for HobbesError {
    fn from(value: num::ParseIntError) -> Self {
        HobbesError::ParseIntError(value)
    }
}

impl From<subscriber::SetGlobalDefaultError> for HobbesError {
    fn from(value: subscriber::SetGlobalDefaultError) -> Self {
        HobbesError::SetGlobalDefaultError(value)
    }
}

impl From<sled::Error> for HobbesError {
    fn from(value: sled::Error) -> Self {
        HobbesError::SledDbError(value)
    }
}
