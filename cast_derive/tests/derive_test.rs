use cast::{Cast, cast, cast_from};

#[test]
fn test_derive() {
    #[derive(Cast, Clone, Copy, Debug, Eq, PartialEq)]
    #[repr(transparent)]
    struct Uuid([u8; 16]);

    let mut bytes = [
        0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9, 0xa, 0xb, 0xc, 0xd, 0xe, 0xf,
    ];
    let mut uuid: Uuid = cast_from(bytes);
    assert_eq!(uuid.0, bytes);
    assert_eq!(cast(uuid), bytes);
    assert_eq!(cast(&uuid), &bytes);
    assert_eq!(cast(&mut uuid), &mut bytes);
    assert_eq!(&uuid, cast_from::<&Uuid, _>(&bytes));
    assert_eq!(&mut uuid, cast_from::<&mut Uuid, _>(&mut bytes));

    assert_eq!(cast(Box::new(uuid)), Box::new(bytes));
    assert_eq!(Box::new(uuid), cast_from(Box::new(bytes)));

    assert_eq!(cast(vec![uuid]), vec![bytes]);
    assert_eq!(vec![uuid], cast_from::<Vec<Uuid>, _>(vec![bytes]));
    assert_eq!(cast(vec![Box::new(uuid)]), vec![Box::new(bytes)]);
    assert_eq!(vec![Box::new(uuid)], cast_from::<Vec<Box<Uuid>>, _>(vec![Box::new(bytes)]));
}

#[test]
fn test_derive_generic() {
    #[derive(Cast, Clone, Copy, Debug, Eq, PartialEq)]
    #[repr(transparent)]
    struct Uuid<T>([T; 16]);

    let mut bytes = [
        0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9, 0xa, 0xb, 0xc, 0xd, 0xe, 0xf,
    ];
    let mut uuid: Uuid<u8> = cast_from(bytes);
    assert_eq!(uuid.0, bytes);
    assert_eq!(cast(uuid), bytes);
    assert_eq!(cast(&uuid), &bytes);
    assert_eq!(cast(&mut uuid), &mut bytes);
    assert_eq!(&uuid, cast_from::<&Uuid<u8>, _>(&bytes));
    assert_eq!(&mut uuid, cast_from::<&mut Uuid<u8>, _>(&mut bytes));

    assert_eq!(cast(Box::new(uuid)), Box::new(bytes));
    assert_eq!(Box::new(uuid), cast_from(Box::new(bytes)));

    assert_eq!(cast(vec![uuid]), vec![bytes]);
    assert_eq!(vec![uuid], cast_from::<Vec<Uuid<u8>>, _>(vec![bytes]));
    assert_eq!(cast(vec![Box::new(uuid)]), vec![Box::new(bytes)]);
    assert_eq!(vec![Box::new(uuid)], cast_from::<Vec<Box<Uuid<u8>>>, _>(vec![Box::new(bytes)]));
}

#[test]
fn test_derive_unsized() {
    #[derive(Cast, Debug, Eq, PartialEq)]
    #[repr(transparent)]
    struct Slice([u8]);

    impl Clone for Box<Slice> {
        fn clone(&self) -> Self {
            cast_from(self.0.to_owned().into_boxed_slice())
        }
    }

    let mut bytes = vec![1u8, 2, 3, 4].into_boxed_slice();
    let mut slice: Box<Slice> = cast_from(bytes.clone());
    assert_eq!(cast(&*slice), &*bytes);
    assert_eq!(&*slice, cast_from::<&Slice, _>(&*bytes));
    assert_eq!(cast(&mut *slice), &mut *bytes);
    assert_eq!(&mut *slice, cast_from::<&mut Slice, _>(&mut *bytes));
    assert_eq!(cast(vec![slice.clone()]), vec![bytes.clone()]);
    assert_eq!(vec![slice.clone()], cast_from::<Vec<Box<Slice>>, _>(vec![bytes.clone()]));
}

#[test]
fn test_derive_unsized_generic() {
    #[derive(Cast, Debug, Eq, PartialEq)]
    #[repr(transparent)]
    struct Slice<T>([T]);

    impl<T: Clone> Clone for Box<Slice<T>> {
        fn clone(&self) -> Self {
            cast_from(self.0.to_owned().into_boxed_slice())
        }
    }

    let mut bytes = vec![1u8, 2, 3, 4].into_boxed_slice();
    let mut slice: Box<Slice<u8>> = cast_from(bytes.clone());
    assert_eq!(cast(&*slice), &*bytes);
    assert_eq!(&*slice, cast_from::<&Slice<u8>, _>(&*bytes));
    assert_eq!(cast(&mut *slice), &mut *bytes);
    assert_eq!(&mut *slice, cast_from::<&mut Slice<u8>, _>(&mut *bytes));
    assert_eq!(cast(vec![slice.clone()]), vec![bytes.clone()]);
    assert_eq!(vec![slice.clone()], cast_from::<Vec<Box<Slice<u8>>>, _>(vec![bytes.clone()]));
}
