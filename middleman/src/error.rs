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
    InvalidInput,
    Busy,
    Unexpected,
    NetworkError,
}

pub type Result<T> = std::result::Result<T, Box<Error>>;

impl ErrorKind {
    fn status_code(&self) -> StatusCode {
        unsafe {
            std::mem::transmute::<u16, _>(match self {
                Self::InvalidInput => 400,
                Self::Busy => 409,
                Self::Unexpected => 500,
                // Not sure if 502 makes the most sense here
                Self::NetworkError => 502,
            })
        }
    }
}

impl std::fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidInput => write!(f, "invalid input"),
            Self::Busy => write!(f, "busy, try again"),
            Self::Unexpected => write!(f, "internal error"),
            Self::NetworkError => write!(f, "network request failed"),
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

    pub(crate) fn from_kind(kind: ErrorKind) -> Self {
        Self { kind, cause: None }
    }

    pub(crate) fn with_cause(
        kind: ErrorKind,
        string: impl Into<Box<dyn StdError + Send + Sync + 'static>>,
    ) -> Self {
        Self {
            kind,
            cause: Some(string.into().into()),
        }
    }
}

impl std::error::Error for Error {}

macro_rules! define_errors {
    ($($ty:ty => $kind:expr),*$(,)?) => {
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

define_errors! {
    String => ErrorKind::Unexpected,
    std::io::Error => ErrorKind::Unexpected,
    std::net::AddrParseError => ErrorKind::InvalidInput,
    std::num::ParseIntError => ErrorKind::InvalidInput,
    std::env::VarError => ErrorKind::InvalidInput,
    std::str::Utf8Error => ErrorKind::InvalidInput,
    tokio_native_tls::native_tls::Error => ErrorKind::NetworkError,
    hyper::Error => ErrorKind::NetworkError,
    hyper_util::client::legacy::connect::dns::InvalidNameError => ErrorKind::InvalidInput,
}

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
        Box::new(Error::from_kind(value))
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
