use std::error::Error as StdError;
use std::result::Result as StdResult;

use axum::response::IntoResponse;
use axum::Json;
use hyper::StatusCode;
use serde_derive::{Deserialize, Serialize};

/// General-purpose error type
// XXX: I'm sure you can avoid allocation when there's no underlying error.
#[derive(Debug)]
pub struct Error {
    inner: Box<ErrorInner>,
}

#[derive(Debug)]
struct ErrorInner {
    pub(crate) kind: ErrorKind,
    pub(crate) cause: Option<Box<dyn StdError + Send + Sync + 'static>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ErrorKind {
    Busy,
    Unexpected,
}

pub type Result<T> = StdResult<T, Error>;

pub type DynError = Error;
pub type DynResult<T> = Result<T>;

impl ErrorKind {
    fn status_code(&self) -> StatusCode {
        unsafe {
            std::mem::transmute(match self {
                Self::Busy => 409u16,
                Self::Unexpected => 500,
            })
        }
    }
}

impl std::fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Busy => write!(f, "busy, try again"),
            Self::Unexpected => write!(f, "internal error"),
        }
    }
}

impl Error {
    pub(crate) fn kind(&self) -> ErrorKind {
        self.inner.kind
    }

    pub(crate) fn cause(&self) -> Option<&(dyn StdError + Send + Sync + 'static)> {
        self.inner.cause.as_ref().map(|x| &**x)
    }
}

impl<E: Into<Box<dyn StdError + Send + Sync + 'static>>> From<E> for Error {
    fn from(value: E) -> Self {
        Self {
            inner: Box::new(ErrorInner {
                kind: ErrorKind::Unexpected,
                cause: Some(value.into()),
            }),
        }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.inner.kind)?;
        if let Some(ref cause) = self.cause() {
            write!(f, ": {}", cause)?;
        }
        Ok(())
    }
}

impl From<ErrorKind> for Error {
    fn from(value: ErrorKind) -> Self {
        Self {
            inner: Box::new(ErrorInner {
                kind: value,
                cause: None,
            }),
        }
    }
}

impl IntoResponse for Error {
    fn into_response(self) -> axum::response::Response {
        #[derive(Clone, Debug, Serialize, Deserialize)]
        struct JsonError {
            message: String,
        }

        let mut response = Json(JsonError {
            message: self.to_string(),
        })
        .into_response();
        *response.status_mut() = self.inner.kind.status_code();

        response
    }
}
