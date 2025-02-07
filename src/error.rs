use thiserror::Error;

#[derive(Error, Debug)]
pub enum FileSystemError {
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Opendal error: {0}")]
    OpendalError(#[from] opendal::Error),
    #[error("File not found: {0}")]
    FileNotFound(String),
    #[error("Invalid file path: {0}")]
    InvalidPath(String),
    #[error("Not supported: {0}")]
    NotSupported(String),
}

pub type IoResult<T> = std::result::Result<T, FileSystemError>;