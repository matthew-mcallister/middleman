use std::error::Error as StdError;
use std::result::Result as StdResult;

use axum::response::IntoResponse;
use axum::Json;
use serde_derive::{Deserialize, Serialize};

/// General-purpose error type
#[derive(Debug)]
pub struct Error {
    inner: Box<Box<dyn StdError + Send + Sync + 'static>>,
}

pub type Result<T> = StdResult<T, Error>;

pub type DynError = Error;
pub type DynResult<T> = Result<T>;

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

impl<E: Into<Box<dyn StdError + Send + Sync + 'static>>> From<E> for Error {
    fn from(value: E) -> Self {
        Self {
            inner: Box::new(value.into()),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct JsonError {
    message: String,
}

impl IntoResponse for Error {
    fn into_response(self) -> axum::response::Response {
        Json(JsonError {
            message: self.inner.to_string(),
        })
        .into_response()
    }
}

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

impl StdError for CorruptedRecordError {}
