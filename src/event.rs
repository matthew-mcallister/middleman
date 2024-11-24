use std::sync::atomic::{AtomicU64, Ordering};

use crate::error::DynResult;
use crate::types::{DbTransaction, IdempotencyKey, Prefix, Tag};
use crate::util::ByteCast;
use crate::{impl_byte_cast_unsized, make_dst, make_key};

// XXX: Probably just replace with string interning
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ContentType {
    Json,
}

impl std::fmt::Display for ContentType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match *self {
                ContentType::Json => "application/json",
            }
        )
    }
}

/// Immutable event data structure.
///
/// # Layout
///
/// ```ignore
///   [header]
///   [0 stream]
///   [1 payload]
/// ```
#[derive(Debug, PartialEq, Eq)]
#[repr(C)]
pub struct EventT<T: ?Sized> {
    idempotency_key: IdempotencyKey,
    tag: Tag,
    _flags: u32,
    _reserved: u32,
    // Allows adding extensions in the future
    _next_header: u32,
    offsets: [u32; 1],
    content: T,
}

pub type Event = EventT<[u8]>;

impl_byte_cast_unsized!(EventT, content);

impl Event {
    pub fn content_type(&self) -> ContentType {
        // XXX: Read content type from flags
        ContentType::Json
    }

    pub fn idempotency_key(&self) -> IdempotencyKey {
        self.idempotency_key
    }

    pub(crate) fn idempotency_lookup_key(&self) -> [u8; 1 + 16 + 16] {
        make_key!(
            u8: Prefix::EventIdempotencyKeyIndex as u8,
            u128: self.tag(),
            u128: self.idempotency_key(),
        )
    }

    pub fn tag(&self) -> Tag {
        self.tag
    }

    pub fn stream(&self) -> &str {
        unsafe { std::str::from_utf8_unchecked(&self.content[..self.offsets[0] as usize]) }
    }

    pub fn payload(&self) -> &[u8] {
        &self.content[self.offsets[0] as usize..]
    }
}

#[derive(Debug, Default)]
pub struct EventBuilder<'a> {
    idempotency_key: IdempotencyKey,
    tag: Tag,
    content_type: Option<ContentType>,
    stream: &'a str,
    payload: &'a [u8],
}

impl<'a> EventBuilder<'a> {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn content_type(&mut self, ty: ContentType) -> &mut Self {
        self.content_type = Some(ty);
        self
    }

    pub fn idempotency_key(&mut self, key: impl Into<IdempotencyKey>) -> &mut Self {
        self.idempotency_key = key.into();
        self
    }

    pub fn tag(&mut self, tag: Tag) -> &mut Self {
        self.tag = tag;
        self
    }

    pub fn stream(&mut self, stream: &'a str) -> &mut Self {
        self.stream = stream;
        self
    }

    pub fn payload(&mut self, payload: &'a [u8]) -> &mut Self {
        self.payload = payload;
        self
    }

    pub fn build(&mut self) -> Box<Event> {
        unsafe {
            make_dst!(EventT[u8] {
                _reserved: 0,
                _next_header: 0,
                _flags: 0,
                tag: self.tag,
                idempotency_key: self.idempotency_key,
                offsets: [self.stream.len() as u32],
                [content]: (self.stream, self.payload),
            })
        }
    }
}

pub(crate) struct EventTable {
    event_sequence_number: AtomicU64,
}

impl EventTable {
    pub fn new() -> Self {
        Self {
            // FIXME: Have to initialize the sequence number from the DB
            event_sequence_number: AtomicU64::new(0),
        }
    }

    /// Creates an event. Event creation is atomic but *not* synchronized,
    /// meaning that events from different sources or partitions are not
    /// guaranteed to be ordered.
    pub fn create(&self, txn: &DbTransaction<'_>, event: &Event) -> DynResult<u64> {
        // XXX: Column family!

        // Create primary record
        let id = self.event_sequence_number.fetch_add(1, Ordering::Relaxed);
        let key = make_key!(u8: Prefix::Event as u8, u64: id);
        unsafe { txn.put(&key, ByteCast::as_bytes(event))? };

        // Index by tag + idempotency key
        txn.put(&event.idempotency_lookup_key(), &id.to_ne_bytes())?;

        // Index by tag + stream
        let mut key = vec![Prefix::EventStreamIndex as u8];
        key.extend(event.tag().to_ne_bytes());
        key.extend(event.stream().as_bytes());
        txn.put(key, &id.to_ne_bytes())?;

        Ok(id)
    }
}

#[cfg(test)]
mod tests {
    use crate::make_key;
    use crate::testing::TestHarness;
    use crate::types::Prefix;
    use crate::util::ByteCast;

    use super::{ContentType, EventBuilder, EventTable};

    #[test]
    fn test_builder() {
        let stream = "my_stream";
        let tag = 12345;
        let payload = b"1234ideclareathumbwar";
        let event = EventBuilder::new()
            .content_type(ContentType::Json)
            .stream(stream)
            .tag(tag)
            .payload(payload)
            .build();
        assert_eq!(event.content_type(), ContentType::Json);
        assert_eq!(event.stream(), stream);
        assert_eq!(event.tag(), tag);
        assert_eq!(event.payload(), payload);
    }

    #[test]
    fn test_create() {
        let mut harness = TestHarness::new();
        let db = harness.db();

        let events = EventTable::new();
        let txn = db.transaction();
        let event = EventBuilder::new()
            .content_type(ContentType::Json)
            .tag(0)
            .stream("asdf")
            .payload(b"1234321")
            .idempotency_key(1212u128)
            .build();
        events.create(&txn, &event).unwrap();
        txn.commit().unwrap();

        let key = make_key!(u8: Prefix::Event as u8, u64: 0);
        let bytes = db.get(&key).unwrap().unwrap();
        unsafe { assert_eq!(ByteCast::as_bytes(&*event), bytes) };

        let event2 = unsafe { ByteCast::from_bytes_owned(bytes) };
        assert_eq!(event, event2);
    }
}
