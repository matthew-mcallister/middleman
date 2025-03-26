use std::fmt::Display;

use axum::body::Body;
use axum::response::{IntoResponse, Response};
use http::HeaderValue;
use serde::Serialize;

macro_rules! wrapper {
    (
        $(#[$($meta:tt)*])*
        $vis:vis $Wrapper:ident;
    ) => {
        $(#[$($meta)*])*
        #[repr(transparent)]
        $vis struct $Wrapper<T: ?Sized>($vis T);

        impl<'a, T: ?Sized> From<$Wrapper<&'a T>> for &'a $Wrapper<T> {
            fn from(value: $Wrapper<&'a T>) -> Self {
                unsafe { std::mem::transmute(value) }
            }
        }

        impl<'a, T: ?Sized> From<$Wrapper<&'a mut T>> for &'a mut $Wrapper<T> {
            fn from(value: $Wrapper<&'a mut T>) -> Self {
                unsafe { std::mem::transmute(value) }
            }
        }

        impl<'a, T> From<$Wrapper<&'a [T]>> for &'a [$Wrapper<T>] {
            fn from(value: $Wrapper<&'a [T]>) -> Self {
                unsafe { std::mem::transmute(value) }
            }
        }

        impl<'a, T> From<$Wrapper<&'a mut [T]>> for &'a mut [$Wrapper<T>] {
            fn from(value: $Wrapper<&'a mut [T]>) -> Self {
                unsafe { std::mem::transmute(value) }
            }
        }

        impl<T: ?Sized> From<$Wrapper<Box<T>>> for Box<$Wrapper<T>> {
            fn from(value: $Wrapper<Box<T>>) -> Self {
                unsafe { std::mem::transmute(value) }
            }
        }

        impl<T: ?Sized> From<Box<$Wrapper<T>>> for $Wrapper<Box<T>> {
            fn from(value: Box<$Wrapper<T>>) -> Self {
                unsafe { std::mem::transmute(value) }
            }
        }

        impl<T> From<$Wrapper<Vec<T>>> for Vec<$Wrapper<T>> {
            fn from(value: $Wrapper<Vec<T>>) -> Self {
                unsafe { std::mem::transmute(value) }
            }
        }

        impl<T> From<Vec<$Wrapper<T>>> for $Wrapper<Vec<T>> {
            fn from(value: Vec<$Wrapper<T>>) -> Self {
                unsafe { std::mem::transmute(value) }
            }
        }

        #[allow(dead_code)]
        impl<T: ?Sized> $Wrapper<T> {
            $vis fn from_ref(val: &T) -> &Self {
                $Wrapper(val).into()
            }

            $vis fn from_mut(val: &mut T) -> &mut Self {
                $Wrapper(val).into()
            }

            $vis fn from_box(val: Box<T>) -> Box<Self> {
                $Wrapper(val).into()
            }
        }

        #[allow(dead_code)]
        impl<T> $Wrapper<T> {
            $vis fn from_slice(val: &[T]) -> &[Self] {
                $Wrapper(val).into()
            }

            $vis fn from_slice_mut(val: &mut [T]) -> &mut [Self] {
                $Wrapper(val).into()
            }

            $vis fn from_vec(val: Vec<T>) -> Vec<Self> {
                $Wrapper(val).into()
            }
        }
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
            write!(f, "{}", JsonFormatter::from_ref(e))?;
        }
        write!(f, "]")
    }
}

impl<T: ?Sized> Display for JsonFormatter<Box<T>>
where
    JsonFormatter<T>: Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", JsonFormatter::from_ref(&*self.0))
    }
}

impl<T> Display for JsonFormatter<Vec<T>>
where
    JsonFormatter<T>: Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", JsonFormatter::from_ref(&self.0[..]))
    }
}

impl<T> IntoResponse for JsonFormatter<T>
where
    Self: Display,
{
    fn into_response(self) -> Response {
        let body = self.to_string();
        let mut response = Response::new(Body::new(body));
        response.headers_mut().insert(
            "Content-Type",
            HeaderValue::from_str("application/json").unwrap(),
        );
        response
    }
}

macro_rules! impl_serializer {
    ($Wrapper:ident) => {
        impl<T> IntoResponse for $Wrapper<T>
        where
            Self: Serialize,
        {
            fn into_response(self) -> Response<Body> {
                let body = serde_json::to_string(&self).unwrap();
                let mut response = Response::new(Body::new(body));
                response.headers_mut().insert(
                    "Content-Type",
                    HeaderValue::from_str("application/json").unwrap(),
                );
                response
            }
        }

        impl<T: ?Sized> Serialize for $Wrapper<Box<T>>
        where
            $Wrapper<T>: Serialize,
        {
            fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                $Wrapper::from_ref(&*self.0).serialize(serializer)
            }
        }

        impl<T> Serialize for $Wrapper<[T]>
        where
            $Wrapper<T>: Serialize,
        {
            fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                $Wrapper::from_slice(&self.0).serialize(serializer)
            }
        }

        impl<T> Serialize for $Wrapper<Vec<T>>
        where
            $Wrapper<T>: Serialize,
        {
            fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                $Wrapper::from_slice(&self.0[..]).serialize(serializer)
            }
        }
    };
}

wrapper! {
    /// Helper for serializing structs in admin API format.
    #[derive(Debug)]
    pub(crate) ProducerApiSerializer;
}

impl_serializer!(ProducerApiSerializer);

wrapper! {
    /// Helper for serializing structs in admin API format.
    #[derive(Debug)]
    pub(crate) ConsumerApiSerializer;
}

impl_serializer!(ConsumerApiSerializer);
