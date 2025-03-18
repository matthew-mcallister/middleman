use axum::body::Body;
use axum::response::{IntoResponse, Response};
use http::HeaderValue;

/// Trait for generating JSON responses without using Serde.
pub(crate) trait WriteJson {
    fn write_json(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result;
}

#[derive(Clone, Debug)]
pub(crate) struct ToJson<T: ?Sized>(pub(crate) T);

impl<T: WriteJson + ?Sized> std::fmt::Display for ToJson<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.write_json(f)
    }
}

impl<T: WriteJson> IntoResponse for ToJson<T> {
    fn into_response(self) -> Response<Body> {
        let mut response = Response::new(Body::new(self.to_string()));
        response.headers_mut().insert(
            "Content-Type",
            HeaderValue::from_str("application/json").unwrap(),
        );
        response
    }
}

impl<T: WriteJson + ?Sized> WriteJson for Box<T> {
    fn write_json(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        <T as WriteJson>::write_json(self, f)
    }
}

impl<T: WriteJson> WriteJson for [T] {
    fn write_json(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "[")?;
        let mut first = false;
        for x in self.iter() {
            if !first {
                write!(f, ",")?;
            }
            x.write_json(f)?;
            first = false;
        }
        write!(f, "]")
    }
}

impl<T: WriteJson> WriteJson for Vec<T> {
    fn write_json(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        self[..].write_json(f)
    }
}

impl<'a, T: WriteJson> WriteJson for &'a T {
    fn write_json(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        (*self).write_json(f)
    }
}
