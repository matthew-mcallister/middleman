#![feature(trace_macros)]

use bytecast::{FromBytes, FromBytesError, HasLayout, IntoBytes, IntoBytesMut, box_from_bytes};

#[test]
fn test_struct_with_padding() {
    #[derive(Clone, Copy, Debug, Eq, FromBytes, HasLayout, PartialEq)]
    #[repr(C)]
    struct Inner {
        a: u32,
        b: u16,
    }

    #[derive(Debug, Eq, FromBytes, HasLayout, PartialEq)]
    #[repr(C)]
    struct MyStruct {
        inner: Inner,
        c: [u64],
    }

    let bytes = vec![1, 0, 0, 0, 2, 0, 9, 32, 0, 1, 0, 0, 0, 0, 0, 0];

    let my_struct = MyStruct::ref_from_bytes(&bytes[..]).unwrap();
    assert_eq!(my_struct.inner.a, 1);
    assert_eq!(my_struct.inner.b, 2);
    assert_eq!(&my_struct.c[..], &[256]);

    let my_struct = MyStruct::ref_from_bytes(&bytes[..8]).unwrap();
    assert_eq!(&my_struct.c[..], &[]);

    assert_eq!(MyStruct::ref_from_bytes(&bytes[..7]), Err(FromBytesError::InvalidSize));
    assert_eq!(MyStruct::ref_from_bytes(&bytes[..9]), Err(FromBytesError::InvalidSize));
    assert_eq!(MyStruct::ref_from_bytes(&bytes[1..9]), Err(FromBytesError::InvalidAlignment));
}

#[test]
fn test_struct_into_bytes() {
    #[derive(Debug, Eq, FromBytes, HasLayout, IntoBytes, PartialEq)]
    #[repr(C)]
    struct MyStruct {
        a: u8,
        b: u8,
        c: [u16],
    }
    let bytes = vec![1, 2, 3, 0];
    let my_struct = MyStruct::ref_from_bytes(&bytes[..]).unwrap();
    assert_eq!(my_struct.a, 1);
    assert_eq!(my_struct.b, 2);
    assert_eq!(&my_struct.c[..], &[3]);

    assert_eq!(MyStruct::ref_from_bytes(&bytes[..3]), Err(FromBytesError::InvalidSize));

    let mut my_struct: Box<MyStruct> = box_from_bytes(bytes.into_boxed_slice()).unwrap();
    assert_eq!(my_struct.as_bytes(), &[1, 2, 3, 0]);
    my_struct.a = 5;
    assert_eq!(my_struct.as_bytes(), &[5, 2, 3, 0]);
    my_struct.as_bytes_mut()[3] = 1;
    assert_eq!(my_struct.as_bytes(), &[5, 2, 3, 1]);
    assert_eq!(my_struct.c[0], 259);
}

#[test]
fn test_tail_with_no_padding() {
    #[derive(Debug, Eq, FromBytes, HasLayout, IntoBytes, PartialEq)]
    #[repr(C)]
    struct MyStruct {
        a: u32,
        b: [(u16, u16)],
    }

    let bytes = vec![1, 0, 0, 0, 2, 0, 3, 0];
    let my_struct = MyStruct::ref_from_bytes(&bytes[..]).unwrap();
    assert_eq!(my_struct.a, 1);
    assert_eq!(&my_struct.b[..], &[(2, 3)]);
    assert_eq!(my_struct.as_bytes(), &bytes);
}

#[test]
fn test_generic() {
    #[derive(Debug, Eq, FromBytes, HasLayout, IntoBytes, PartialEq)]
    #[repr(packed)]
    struct Pair<T, U>(T, U);

    let bytes: [u8; 3] = [1, 2, 0];
    let pair: &Pair<u8, u16> = Pair::ref_from_bytes(&bytes[..]).unwrap();
    let Pair(a, b) = *pair;
    assert_eq!(a, 1);
    assert_eq!(b, 2);
    assert_eq!(pair.as_bytes(), bytes);
}

#[test]
fn test_zst() {
    #[derive(HasLayout)]
    #[repr(C)]
    struct Struct {
        a: u8,
        b: [u32; 0],
    }

    assert_eq!(Struct::LAYOUT.size, 4);
    assert_eq!(Struct::LAYOUT.alignment.get(), 4);
}

#[test]
fn test_repr_align() {
    #[derive(HasLayout)]
    #[repr(C, align(4))]
    struct Struct {
        a: u8,
    }

    #[derive(HasLayout)]
    #[repr(C)]
    #[repr(align(4))]
    struct Struct2 {
        a: u8,
    }

    assert_eq!(Struct::LAYOUT.size, 4);
    assert_eq!(Struct::LAYOUT.alignment.get(), 4);
    assert_eq!(Struct2::LAYOUT, Struct::LAYOUT);
}
