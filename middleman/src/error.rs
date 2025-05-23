use std::any::Any;
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
    Unauthenticated,
    Forbidden,
    NotFound,
    Timeout,
    NetworkError,
}

pub type Result<T> = std::result::Result<T, Box<Error>>;

impl ErrorKind {
    fn status_code(&self) -> StatusCode {
        StatusCode::try_from(match self {
            Self::InvalidInput => 400,
            Self::Busy => 409,
            Self::Unexpected => 500,
            Self::Unauthenticated => 401,
            Self::Forbidden => 403,
            Self::NotFound => 404,
            Self::Timeout => 408,
            // Not sure if 502 makes the most sense here
            Self::NetworkError => 502,
        })
        .unwrap()
    }
}

impl std::fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidInput => write!(f, "invalid input"),
            Self::Busy => write!(f, "busy, try again"),
            Self::Unexpected => write!(f, "internal error"),
            Self::Unauthenticated => write!(f, "unauthenticated"),
            Self::Forbidden => write!(f, "forbidden"),
            Self::NotFound => write!(f, "not found"),
            Self::Timeout => write!(f, "timed out"),
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
        cause: impl Into<Box<dyn StdError + Send + Sync + 'static>>,
    ) -> Self {
        Self {
            kind,
            cause: Some(cause.into().into()),
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
    regex::Error => ErrorKind::InvalidInput,
    url::ParseError => ErrorKind::InvalidInput,
    sqlx::Error => ErrorKind::Unexpected,
    uuid::Error => ErrorKind::InvalidInput,
    http::header::ToStrError => ErrorKind::InvalidInput,
    jsonwebtoken::errors::Error => ErrorKind::Unauthenticated,
    tokio::time::error::Elapsed => ErrorKind::Timeout,
    toml::de::Error => ErrorKind::InvalidInput,
}

define_errors! {
    std::convert::Infallible => unreachable!(),
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

// Conversion for catch_unwind
impl From<Box<dyn Any + Send + 'static>> for Box<Error> {
    fn from(_: Box<dyn Any + Send + 'static>) -> Self {
        Box::new(Error {
            kind: ErrorKind::Unexpected,
            cause: Some("unexpected panic".into()),
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
