use std::error::Error as StdError;

#[derive(Debug)]
pub struct Error {
    kind: ErrorKind,
    cause: Option<Box<dyn StdError + Send + Sync + 'static>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ErrorKind {
    /// The operation conflicted with another transaction.
    TransactionConflict,
    /// A corrupted or invalid value was encountered.
    DataError,
    /// The underlying storage library, operating system, or hardware failed.
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

    pub(crate) fn data_error() -> Self {
        Self {
            kind: ErrorKind::DataError,
            cause: None,
        }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.kind {
            ErrorKind::TransactionConflict => write!(f, "conflict with other transaction"),
            ErrorKind::DataError => write!(f, "invalid or corrupted data"),
            ErrorKind::StorageError => write!(f, "{}", self.cause.as_ref().unwrap()),
        }
    }
}

impl StdError for Error {}

macro_rules! convert_errors {
    ($kind:expr; $($ty:ty),*$(,)?) => {
        $(
            impl From<$ty> for Box<Error> {
                fn from(value: $ty) -> Self {
                    Box::new(Error {
                        kind: $kind,
                        cause: Some(value.into()),
                    })
                }
            }
        )*
    };
}

convert_errors!(ErrorKind::StorageError; rocksdb::Error);
convert_errors!(ErrorKind::DataError; std::array::TryFromSliceError);
