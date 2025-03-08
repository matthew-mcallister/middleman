use std::error::Error as StdError;

use axum::response::IntoResponse;
use axum::Json;
use hyper::StatusCode;
use middleman_db as db;
use serde_derive::{Deserialize, Serialize};

/// General-purpose error type
#[derive(Debug)]
pub struct Error {
    kind: ErrorKind,
    cause: Option<Box<dyn StdError + Send + Sync + 'static>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ErrorKind {
    Busy,
    Unexpected,
}

pub type Result<T> = std::result::Result<T, Box<Error>>;

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
        self.kind
    }

    pub(crate) fn cause(&self) -> Option<&(dyn StdError + Send + Sync + 'static)> {
        self.cause.as_ref().map(|x| &**x)
    }
}

macro_rules! unexpected_errors {
    ($($ty:ty),*$(,)?) => {
        $(
            impl From<$ty> for Box<Error> {
                fn from(value: $ty) -> Self {
                    Box::new(Error {
                        kind: ErrorKind::Unexpected,
                        cause: Some(value.into()),
                    })
                }
            }
        )*
    };
}

unexpected_errors!(
    String,
    std::io::Error,
    std::net::AddrParseError,
    std::num::ParseIntError,
    std::env::VarError,
);

impl<'a> From<&'a str> for Box<Error> {
    fn from(value: &'a str) -> Self {
        Box::new(Error {
            kind: ErrorKind::Unexpected,
            cause: Some(value.into()),
        })
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.kind)?;
        if let Some(ref cause) = self.cause() {
            write!(f, ": {}", cause)?;
        }
        Ok(())
    }
}

impl From<ErrorKind> for Box<Error> {
    fn from(value: ErrorKind) -> Self {
        Box::new(Error {
            kind: value,
            cause: None,
        })
    }
}

impl From<Box<db::Error>> for Box<Error> {
    fn from(value: Box<db::Error>) -> Self {
        let kind = match value.kind() {
            db::ErrorKind::TransactionConflict => ErrorKind::Busy,
            _ => ErrorKind::Unexpected,
        };
        Box::new(Error {
            kind,
            cause: Some(value.into()),
        })
    }
}

impl IntoResponse for Box<Error> {
    fn into_response(self) -> axum::response::Response {
        #[derive(Clone, Debug, Serialize, Deserialize)]
        struct JsonError {
            message: String,
        }

        let mut response = Json(JsonError {
            message: self.to_string(),
        })
        .into_response();
        *response.status_mut() = self.kind.status_code();

        response
    }
}
