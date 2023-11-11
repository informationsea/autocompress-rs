use thiserror::Error;

/// Error type for this crate
#[derive(Error, Debug)]
pub enum Error {
    #[error("I/O Error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Decompress Error: {0}")]
    DecompressError(String),
    #[error("Compress Error: {0}")]
    CompressError(String),
    #[error("More data required to finish")]
    MoreDataRequired,
    #[error("Channel Send Error")]
    SendError,
}

/// Result type for this crate
pub type Result<T> = std::result::Result<T, Error>;

impl From<Error> for std::io::Error {
    fn from(e: Error) -> Self {
        match e {
            Error::IoError(e) => e,
            _ => std::io::Error::new(std::io::ErrorKind::Other, e),
        }
    }
}
