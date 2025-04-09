use std::fmt::Display;

use axum::body::Body;
use axum::response::{IntoResponse, Response};
use http::HeaderValue;
use serde::Serialize;

/// Marker trait for safe two-way transmutation.
///
/// # Safety
///
/// If `T: Cast<U>`, then T and U must be safely convertible using
/// `std::mem::transmute` and it must be safe to access aliased `&T` and `&U`
/// references.
pub(crate) unsafe trait Cast<T: ?Sized> {}

unsafe impl<'a, T: ?Sized, U: ?Sized> Cast<&'a U> for &'a T where T: Cast<U> {}
unsafe impl<'a, T: ?Sized, U: ?Sized> Cast<&'a mut U> for &'a mut T where T: Cast<U> {}
unsafe impl<T: ?Sized, U: ?Sized> Cast<Box<U>> for Box<T> where T: Cast<U> {}
unsafe impl<T, U> Cast<[U]> for [T] where T: Cast<U> {}
unsafe impl<T, U> Cast<Vec<U>> for Vec<T> where T: Cast<U> {}

/// Safely casts from one type to another.
pub(crate) fn cast<T, U>(val: T) -> U
where
    T: Cast<U>,
{
    let new_val = unsafe { (&val as *const T as *const U).read() };
    std::mem::forget(val);
    new_val
}

/// Safely casts a reference to a possibly unsized type.
pub(crate) fn cast_ref<T: ?Sized, U: ?Sized>(val: &T) -> &U
where
    T: Cast<U>,
{
    unsafe { (&val as *const &T as *const &U).read() }
}

pub(crate) fn cast_mut<T: ?Sized, U: ?Sized>(val: &mut T) -> &mut U
where
    T: Cast<U>,
{
    unsafe { (&val as *const &mut T as *const &mut U).read() }
}

macro_rules! wrapper {
    (
        $(#[$($meta:tt)*])*
        $vis:vis $Wrapper:ident;
    ) => {
        $(#[$($meta)*])*
        #[repr(transparent)]
        $vis struct $Wrapper<T: ?Sized>($vis T);

        unsafe impl<T: ?Sized> Cast<T> for $Wrapper<T> {}
        unsafe impl<T: ?Sized> Cast<$Wrapper<T>> for T {}
    };
}

wrapper! {
    /// Trait for generating JSON responses without using Serde. This is only
    /// needed for events.
    #[derive(Debug)]
    pub(crate) JsonFormatter;
}

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
            write!(f, "{}", cast::<_, &JsonFormatter<_>>(e))?;
        }
        write!(f, "]")
    }
}

impl<T: ?Sized> Display for JsonFormatter<Box<T>>
where
    JsonFormatter<T>: Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", cast::<_, &JsonFormatter<_>>(&*self.0))
    }
}

impl<T> Display for JsonFormatter<Vec<T>>
where
    JsonFormatter<T>: Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", cast::<_, &JsonFormatter<[_]>>(&self.0[..]))
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

wrapper! {
    /// Helper for serializing structs in admin API format.
    #[derive(Debug)]
    pub(crate) ProducerApiSerializer;
}

impl_into_response!(ProducerApiSerializer);

wrapper! {
    /// Helper for serializing structs in admin API format.
    #[derive(Debug)]
    pub(crate) ConsumerApiSerializer;
}

impl_into_response!(ConsumerApiSerializer);
