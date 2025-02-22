use std::error::Error as StdError;

#[derive(Debug)]
pub enum Error {
    TransactionConflict,
    Raw(rocksdb::Error),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ErrorKind {
    TransactionConflict,
    StorageError,
}

pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    pub fn kind(&self) -> ErrorKind {
        match self {
            Self::TransactionConflict => ErrorKind::TransactionConflict,
            Self::Raw(_) => ErrorKind::StorageError,
        }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TransactionConflict => write!(f, "conflict with other transaction"),
            Self::Raw(e) => write!(f, "{}", e),
        }
    }
}

impl StdError for Error {}

impl From<rocksdb::Error> for Error {
    fn from(value: rocksdb::Error) -> Self {
        Self::Raw(value)
    }
}
