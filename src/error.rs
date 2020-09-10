use std::io;
use thiserror::Error;

/// Custom defined `Error` type.
#[derive(Error, Debug)]
pub enum KvsError {
    /// Get key not found error
    #[error("Key not found")]
    KeyNotFound,

    /// Get unexpected command type error
    #[error("Get unexpected command type")]
    UnexpectedCommandType,

    /// Io error
    #[error("Io error")]
    Io(#[from] io::Error),

    /// Serde json error
    #[error("Serde json error")]
    SerdeJson(#[from] serde_json::error::Error),

    /// Sled error
    #[error("Sled error")]
    Sled(#[from] sled::Error),

    /// From utf8 error
    #[error("From utf8 error")]
    FromUtf8(#[from] std::string::FromUtf8Error),

    /// String error
    #[error("String error `{0}`")]
    StringError(String),
}

/// Custom defined `Result` type.
pub type Result<T> = std::result::Result<T, KvsError>;
