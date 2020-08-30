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
    #[error("serde json error")]
    SerdeJson(#[from] serde_json::error::Error),
}

/// Custom defined `Result` type.
pub type Result<T> = std::result::Result<T, KvsError>;
