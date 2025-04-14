use bytecast_derive::{FromBytes, HasLayout, IntoBytes};

#[derive(Clone, Copy, Debug, FromBytes, HasLayout, IntoBytes, PartialEq, Eq)]
#[repr(packed)]
pub struct Unalign<T>(pub T);

impl<T> Unalign<T> {
    pub fn into_inner(self) -> T {
        self.0
    }

    pub fn from_ref(reference: &T) -> &Self {
        unsafe { std::mem::transmute(reference) }
    }

    pub fn from_mut(reference: &mut T) -> &mut Self {
        unsafe { std::mem::transmute(reference) }
    }

    pub fn as_pointer(&self) -> *const T {
        self as *const Self as *const T
    }

    pub fn as_pointer_mut(&mut self) -> *mut T {
        self as *mut Self as *mut T
    }

    pub fn try_as_ref(&self) -> Option<&T> {
        let ptr = self.as_pointer();
        if ptr as usize % std::mem::align_of::<T>() == 0 {
            unsafe { Some(&*ptr) }
        } else {
            None
        }
    }

    pub fn try_as_mut(&mut self) -> Option<&mut T> {
        let ptr = self.as_pointer_mut();
        if ptr as usize % std::mem::align_of::<T>() == 0 {
            unsafe { Some(&mut *ptr) }
        } else {
            None
        }
    }
}
