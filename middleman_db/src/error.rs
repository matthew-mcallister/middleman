use std::error::Error as StdError;

#[derive(Debug)]
pub struct Error {
    kind: ErrorKind,
    cause: Option<rocksdb::Error>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ErrorKind {
    TransactionConflict,
    StorageError,
}

pub type Result<T> = std::result::Result<T, Box<Error>>;

impl Error {
    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    pub(crate) fn transaction_conflict() -> Self {
        Self {
            kind: ErrorKind::TransactionConflict,
            cause: None,
        }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.kind {
            ErrorKind::TransactionConflict => write!(f, "conflict with other transaction"),
            ErrorKind::StorageError => write!(f, "{}", self.cause.as_ref().unwrap()),
        }
    }
}

impl StdError for Error {}

impl From<rocksdb::Error> for Error {
    fn from(value: rocksdb::Error) -> Self {
        Self {
            kind: ErrorKind::StorageError,
            cause: Some(value),
        }
    }
}

impl From<rocksdb::Error> for Box<Error> {
    fn from(value: rocksdb::Error) -> Self {
        Box::new(value.into())
    }
}
