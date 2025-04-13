pub use cast_derive::Cast;

/// Marker trait for safe two-way transmutation that support unsized types.
///
/// # Safety
///
/// If `T: Cast<U>`, then T and U must be safely convertible using
/// `std::mem::transmute` and it must be safe to access aliased `&T` and `&U`
/// references.
pub unsafe trait Cast<T: ?Sized> {}

unsafe impl<'a, T: ?Sized, U: ?Sized> Cast<&'a U> for &'a T where T: Cast<U> {}
unsafe impl<'a, T: ?Sized, U: ?Sized> Cast<&'a mut U> for &'a mut T where T: Cast<U> {}
unsafe impl<T: ?Sized, U: ?Sized> Cast<Box<U>> for Box<T> where T: Cast<U> {}
unsafe impl<T, U> Cast<[U]> for [T] where T: Cast<U> {}
unsafe impl<T, U> Cast<Vec<U>> for Vec<T> where T: Cast<U> {}

/// Safely casts from one type to another.
pub fn cast<T, U>(val: T) -> U
where
    T: Cast<U>,
{
    let new_val = unsafe { (&val as *const T as *const U).read() };
    std::mem::forget(val);
    new_val
}

/// Safely casts from one type to another, in the opposite direction as `cast`.
/// Due to orphan trait rules, it is not always possible to implement `Cast<T>`
/// going in both directions, so this function must be used in many cases.
///
/// Type inference is somewhat broken for this function unfortunately.
pub fn cast_from<T, U>(val: U) -> T
where
    T: Cast<U>,
{
    let new_val = unsafe { (&val as *const U as *const T).read() };
    std::mem::forget(val);
    new_val
}
