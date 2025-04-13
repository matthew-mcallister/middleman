use bitflags::bitflags;
use bytecast::{box_from_bytes, FromBytes, HasLayout, IntoBytes};

use crate::prefix::IsPrefixOf;

bitflags! {
    #[derive(Default)]
    pub struct BigTupleFlags: u8 {
        /// Use 32-bit field offsets.
        const BIG_FIELDS = 0x01;
        /// Align field offsets to 8 bytes.
        // XXX: Should this align to alignof(max_align_t) instead?
        const ALIGNED = 0x04;
    }
}

/// A `BigTuple` is a zero-copy, schemaless data structure for storing
/// variable-length data. Under the hood, BigTuple is just a way to pack
/// multiple byte arrays of different lengths into a single buffer while
/// preserving dictionary ordering.
///
/// `BigTuples` come with multiple layout options. Fields may be packed or
/// aligned. If fields are short enough, then 16-bit field offsets will be used
/// instead of 32-bit offsets.
#[derive(FromBytes, HasLayout, IntoBytes)]
#[repr(C)]
pub struct BigTuple {
    tail: [u8],
}

#[derive(Debug, Default)]
#[non_exhaustive]
pub struct BigTupleCreateInfo<'a> {
    pub aligned: bool,
    pub fields: &'a [&'a [u8]],
}

impl BigTuple {
    pub fn new(create_info: BigTupleCreateInfo<'_>) -> Box<Self> {
        fn pad_with_zeroes<const ALIGN: usize>(vec: &mut Vec<u8>) {
            let desired_length = ALIGN * ((vec.len() + ALIGN - 1) / ALIGN);
            for _ in 0..desired_length - vec.len() {
                vec.push(0);
            }
        }

        assert!(create_info.fields.len() <= u8::MAX as usize);

        let mut flags = BigTupleFlags::default();
        if create_info.aligned {
            flags |= BigTupleFlags::ALIGNED;
        }

        // Calculate offsets
        let mut offset = 0;
        let mut offsets = Vec::<usize>::new();
        for (i, field) in create_info.fields.iter().enumerate() {
            if i > 0 {
                assert!(offset <= u32::MAX as usize);
                offsets.push(offset);
                if create_info.aligned {
                    offset = 8 * ((offset + 7) / 8);
                }
            }
            offset += field.len();
        }

        let big_fields = offsets.iter().any(|&offset| offset > u16::MAX as usize);
        if big_fields {
            flags |= BigTupleFlags::BIG_FIELDS;
        }

        // Write header
        let header_len = if big_fields { 4 * (offsets.len() + 1) } else { 2 * (offsets.len() + 1) };

        let mut bytes = Vec::with_capacity(header_len + offset);
        bytes.push(flags.bits());
        bytes.push(create_info.fields.len() as u8);

        // Write offsets
        if big_fields {
            pad_with_zeroes::<4>(&mut bytes);
            for offset in offsets {
                bytes.extend(&(offset as u32).to_ne_bytes());
            }
        } else {
            for offset in offsets {
                bytes.extend(&(offset as u16).to_ne_bytes());
            }
        }

        // Write fields
        for &field in create_info.fields.iter() {
            if create_info.aligned {
                pad_with_zeroes::<8>(&mut bytes);
            }
            bytes.extend(field);
        }

        box_from_bytes(bytes.into_boxed_slice()).unwrap()
    }

    fn get_offset(&self, index: usize) -> usize {
        let len = self.num_offsets();
        debug_assert!(index < len);
        if self.big_fields() {
            let bytes = <[u8; 4]>::try_from(&self.tail[4 * (index + 1)..4 * (index + 2)]).unwrap();
            u32::from_ne_bytes(bytes) as usize
        } else {
            let bytes = <[u8; 2]>::try_from(&self.tail[2 * (index + 1)..2 * (index + 2)]).unwrap();
            u16::from_ne_bytes(bytes) as usize
        }
    }

    fn flags(&self) -> BigTupleFlags {
        BigTupleFlags::from_bits_retain(self.tail[0])
    }

    fn num_fields(&self) -> u8 {
        self.tail[1]
    }

    fn num_offsets(&self) -> usize {
        if self.num_fields() == 0 {
            0
        } else {
            (self.num_fields() - 1) as usize
        }
    }

    fn big_fields(&self) -> bool {
        self.flags().intersects(BigTupleFlags::BIG_FIELDS)
    }

    fn aligned(&self) -> bool {
        self.flags().intersects(BigTupleFlags::ALIGNED)
    }

    fn payload(&self) -> &[u8] {
        let mut offset = if self.big_fields() {
            4 * (self.num_offsets() as usize + 1)
        } else {
            2 * (self.num_offsets() as usize + 1)
        };
        if self.aligned() {
            offset = 8 * ((offset + 7) / 8);
        }
        &self.tail[offset..]
    }

    /// Returns the number of fields in the tuple.
    pub fn len(&self) -> usize {
        self.num_fields() as usize
    }

    /// Looks up a field by index.
    pub fn get(&self, index: usize) -> &[u8] {
        let payload = self.payload();
        let num_offsets = self.num_offsets();
        assert!(index <= num_offsets);
        let (mut start, end) = if num_offsets == 0 {
            (0, payload.len())
        } else if index == 0 {
            (0, self.get_offset(0))
        } else if index == num_offsets {
            (self.get_offset(index - 1), payload.len())
        } else {
            (self.get_offset(index - 1), self.get_offset(index))
        };
        if self.aligned() {
            start = 8 * ((start + 7) / 8);
        }
        &payload[start..end]
    }

    pub fn get_as<T: FromBytes + ?Sized>(&self, index: usize) -> &T {
        T::ref_from_bytes(self.get(index)).unwrap()
    }

    pub fn iter(&self) -> impl Iterator<Item = &[u8]> + '_ {
        (0..self.len()).map(|i| self.get(i))
    }
}

impl std::fmt::Debug for BigTuple {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut t = f.debug_tuple(stringify!(BigTuple));
        for field in self.iter() {
            t.field(&field);
        }
        t.finish()
    }
}

impl PartialEq for BigTuple {
    fn eq(&self, other: &Self) -> bool {
        self.len() == other.len() && self.iter().zip(other.iter()).all(|(a, b)| a == b)
    }
}

impl Eq for BigTuple {}

impl PartialOrd for BigTuple {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for BigTuple {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        for i in 0.. {
            match (i >= self.len(), i >= other.len()) {
                (true, true) => return std::cmp::Ordering::Equal,
                (true, false) => return std::cmp::Ordering::Less,
                (false, true) => return std::cmp::Ordering::Greater,
                (false, false) => {},
            }
            let ord = self.get(i).cmp(other.get(i));
            match ord {
                std::cmp::Ordering::Equal => continue,
                _ => return ord,
            }
        }
        unreachable!()
    }
}

impl IsPrefixOf for BigTuple {
    fn is_prefix_of(&self, other: &Self) -> bool {
        if other.len() < self.len() {
            return false;
        }
        if self.len() == 0 {
            return true;
        }
        self.iter().take(self.len() - 1).zip(other.iter()).all(|(a, b)| a == b)
            && self.get(self.len() - 1).is_prefix_of(other.get(self.len() - 1))
    }
}

impl ToOwned for BigTuple {
    type Owned = Box<Self>;

    fn to_owned(&self) -> Box<Self> {
        box_from_bytes(self.tail.to_owned().into_boxed_slice()).unwrap()
    }
}

/// Compares two byte strings by reinterpreting them as BigTuples.
pub fn big_tuple_comparator(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
    let a = BigTuple::ref_from_bytes(a).unwrap();
    let b = BigTuple::ref_from_bytes(b).unwrap();
    Ord::cmp(a, b)
}

#[macro_export]
macro_rules! big_tuple {
    ($($expr:expr),*$(,)?) => {{
        let fields = &[$(::bytecast::IntoBytes::as_bytes($expr),)*];
        let mut info = $crate::big_tuple::BigTupleCreateInfo::default();
        info.fields = fields;
        $crate::big_tuple::BigTuple::new(info)
    }};
}

pub use big_tuple;

#[cfg(test)]
mod tests {
    use bytecast::{FromBytes, IntoBytes};

    use crate::prefix::IsPrefixOf;

    use super::{BigTuple, BigTupleCreateInfo};

    #[test]
    fn test_empty() {
        let info = BigTupleCreateInfo {
            fields: &[],
            ..Default::default()
        };
        let tuple = BigTuple::new(info);
        assert_eq!(tuple.len(), 0);
    }

    #[test]
    fn test_unaligned_small() {
        let info = BigTupleCreateInfo {
            fields: &[b"hello", b"world"],
            ..Default::default()
        };
        let tuple = BigTuple::new(info);
        assert_eq!(tuple.len(), 2);
        assert_eq!(tuple.get(0), b"hello");
        assert_eq!(tuple.get(1), b"world");
        assert_eq!(<BigTuple as IntoBytes>::as_bytes(&tuple), b"\x00\x02\x05\x00helloworld");
    }

    #[test]
    #[should_panic]
    fn test_out_of_bounds() {
        let info = BigTupleCreateInfo {
            fields: &[b"hello", b"world"],
            ..Default::default()
        };
        let tuple = BigTuple::new(info);
        tuple.get(2);
    }

    #[test]
    fn test_unaligned_big() {
        let info = BigTupleCreateInfo {
            fields: &[&[0; 67890], &[1; 67890]],
            ..Default::default()
        };
        let tuple = BigTuple::new(info);
        assert_eq!(tuple.len(), 2);
        assert_eq!(tuple.get(0), &[0; 67890]);
        assert_eq!(tuple.get(1), &[1; 67890]);
    }

    #[test]
    fn test_aligned_small() {
        let info = BigTupleCreateInfo {
            fields: &[b"foo", b"bar"],
            aligned: true,
            ..Default::default()
        };
        let tuple = BigTuple::new(info);
        assert_eq!(tuple.len(), 2);
        assert_eq!(tuple.get(0), b"foo");
        assert_eq!(tuple.get(0).as_ptr() as usize % 8, 0);
        assert_eq!(tuple.get(1), b"bar");
        assert_eq!(tuple.get(1).as_ptr() as usize % 8, 0);
    }

    #[test]
    fn test_aligned_big() {
        let info = BigTupleCreateInfo {
            fields: &[&[0; 67890], &[1; 67890]],
            aligned: true,
            ..Default::default()
        };
        let tuple = BigTuple::new(info);
        assert_eq!(tuple.len(), 2);
        assert_eq!(tuple.get(0), &[0; 67890]);
        assert_eq!(tuple.get(0).as_ptr() as usize % 8, 0);
        assert_eq!(tuple.get(1), &[1; 67890]);
        assert_eq!(tuple.get(1).as_ptr() as usize % 8, 0);
    }

    #[test]
    fn test_eq() {
        let tuple = big_tuple!(b"asdf", b"1234");
        assert_eq!(tuple, tuple);
        assert_ne!(tuple, big_tuple!(b"asdf"));
        assert_ne!(tuple, big_tuple!(b"bsdf", b"1234"));
        assert_ne!(tuple, big_tuple!(b"asdf", b"1234", b"5678"));
    }

    #[test]
    fn test_ord() {
        let tuple = big_tuple!(b"asdf", b"1234");
        assert!(tuple <= tuple);
        assert!(tuple < big_tuple!(b"asdf", b"1235"));
        assert!(tuple < big_tuple!(b"asdf", b"1234\0"));
        assert!(tuple < big_tuple!(b"asdf", b"2234"));
        assert!(tuple < big_tuple!(b"asdg", b"1234"));
        assert!(tuple < big_tuple!(b"bsdf", b"1234"));
        assert!(tuple < big_tuple!(b"asdf", b"1234", b""));
        assert!(tuple < big_tuple!(b"asdf", b"1234", b"\0"));

        assert!(tuple > big_tuple!(b"asdf", b"123\0"));
        assert!(tuple > big_tuple!(b"asd"));
        assert!(tuple > big_tuple!(b"asdf"));
        assert!(tuple > big_tuple!(b"asdf", b"123"));
    }

    #[test]
    fn test_from_bytes() {
        let tuple = big_tuple!(b"I", b"love", b"you");
        let bytes = <BigTuple as IntoBytes>::as_bytes(&tuple);
        let round_trip = BigTuple::ref_from_bytes(bytes).unwrap();
        assert_eq!(*tuple, *round_trip);
        assert_eq!(*round_trip, *big_tuple!(b"I", b"love", b"you"));
    }

    #[test]
    fn test_from_bytes_unaligned() {
        let bytes = b"\x69\x00\x02\x05\x00helloworld\x69";
        let tuple = BigTuple::ref_from_bytes(&bytes[1..bytes.len() - 1]).unwrap();
        assert_eq!(tuple.get(0), b"hello");
        assert_eq!(tuple.get(1), b"world");
    }

    #[test]
    fn test_prefix() {
        let tuple = big_tuple!(b"I", b"love", b"you");
        assert!(big_tuple!(b"I", b"love").is_prefix_of(&*tuple));
        assert!(big_tuple!(b"I", b"lo").is_prefix_of(&*tuple));
        assert!(big_tuple!(b"I", b"love", b"y").is_prefix_of(&*tuple));
        assert!(!big_tuple!(b"I", b"lurv", b"you").is_prefix_of(&*tuple));
        assert!(!big_tuple!(b"I", b"lurv").is_prefix_of(&*tuple));
        assert!(!big_tuple!(b"I", b"love", b"youuu").is_prefix_of(&*tuple));
        assert!(!big_tuple!(b"I", b"love", b"you", b"very", b"much").is_prefix_of(&*tuple));
    }
}
