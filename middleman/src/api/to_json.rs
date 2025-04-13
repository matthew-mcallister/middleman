use std::fmt::Display;

use axum::body::Body;
use axum::response::{IntoResponse, Response};
use cast::{cast, cast_from, Cast};
use http::HeaderValue;
use serde::Serialize;

/// Struct for generating JSON responses without using Serde. This is only
/// needed for events since we paste the JSON payload straight into the HTTP
/// response...
#[derive(Debug, Cast)]
#[repr(transparent)]
pub(crate) struct JsonFormatter<T: ?Sized>(pub(crate) T);

impl<T> Display for JsonFormatter<[T]>
where
    JsonFormatter<T>: Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "[")?;
        let mut first = true;
        for e in self.0.iter() {
            if !first {
                write!(f, ",")?;
            }
            first = false;
            write!(f, "{}", cast_from::<&JsonFormatter<_>, _>(e))?;
        }
        write!(f, "]")
    }
}

impl<T: ?Sized> Display for JsonFormatter<Box<T>>
where
    JsonFormatter<T>: Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", cast_from::<&JsonFormatter<_>, _>(&*self.0))
    }
}

impl<T> Display for JsonFormatter<Vec<T>>
where
    JsonFormatter<T>: Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", cast_from::<&JsonFormatter<[_]>, _>(&self.0[..]))
    }
}

impl<T> IntoResponse for JsonFormatter<T>
where
    Self: Display,
{
    fn into_response(self) -> Response {
        let body = self.to_string();
        let mut response = Response::new(Body::new(body));
        response
            .headers_mut()
            .insert("Content-Type", HeaderValue::from_str("application/json").unwrap());
        response
    }
}

macro_rules! impl_into_response {
    ($Wrapper:ident) => {
        impl<T> IntoResponse for $Wrapper<T>
        where
            Self: Serialize,
        {
            fn into_response(self) -> Response<Body> {
                let body = serde_json::to_string(&self).unwrap();
                let mut response = Response::new(Body::new(body));
                response
                    .headers_mut()
                    .insert("Content-Type", HeaderValue::from_str("application/json").unwrap());
                response
            }
        }
    };
}

/// Helper for serializing structs in admin API format.
#[derive(Debug, Cast)]
#[repr(transparent)]
pub(crate) struct ProducerApiSerializer<T: ?Sized>(pub(crate) T);

impl_into_response!(ProducerApiSerializer);

/// Helper for serializing structs in consumer API format.
#[derive(Debug, Cast)]
#[repr(transparent)]
pub(crate) struct ConsumerApiSerializer<T: ?Sized>(pub(crate) T);

impl_into_response!(ConsumerApiSerializer);
