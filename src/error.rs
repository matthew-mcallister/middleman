use std::error::Error;

pub type DynError = Box<dyn Error>;
pub type DynResult<T> = Result<T, DynError>;

/// Error raised when a log file record is invalid or corrupted. In case a log
/// file is damaged by an incomplete append, the error may be recovered by
/// truncating the log file at the corrupt record.
#[derive(Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct CorruptedRecordError;

impl std::fmt::Display for CorruptedRecordError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Corrupted record in log")
    }
}

impl Error for CorruptedRecordError {}
