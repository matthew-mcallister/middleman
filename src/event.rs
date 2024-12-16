use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use uuid::Uuid;

use crate::error::DynResult;
use crate::types::{Db, DbTransaction, Prefix};
use crate::util::{join_slices, ByteCast, DbSlice};
use crate::{define_key, impl_byte_cast_unsized, make_dst};

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
    idempotency_key: Uuid,
    tag: Uuid,
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

    pub fn idempotency_key(&self) -> Uuid {
        self.idempotency_key
    }

    pub fn tag(&self) -> Uuid {
        self.tag
    }

    pub fn stream(&self) -> &str {
        unsafe { std::str::from_utf8_unchecked(&self.content[..self.offsets[0] as usize]) }
    }

    pub fn payload(&self) -> &[u8] {
        &self.content[self.offsets[0] as usize..]
    }

    pub fn as_bytes(&self) -> &[u8] {
        unsafe { ByteCast::as_bytes(self) }
    }
}

impl AsRef<[u8]> for Event {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

#[derive(Debug, Default)]
pub struct EventBuilder<'a> {
    idempotency_key: Uuid,
    tag: Uuid,
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

    pub fn idempotency_key(&mut self, key: impl Into<Uuid>) -> &mut Self {
        self.idempotency_key = key.into();
        self
    }

    pub fn tag(&mut self, tag: Uuid) -> &mut Self {
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

define_key!(EventKey {
    prefix = Prefix::Event,
    id: u64,
});

define_key!(EventIdempotencyKey {
    prefix = Prefix::EventIdempotencyKeyIndex,
    tag: Uuid,
    idempotency_key: Uuid,
});

pub(crate) struct EventTable {
    db: Arc<Db>,
    // XXX: Have to serialize this to the DB, probably once every 256
    // increments or so
    event_sequence_number: AtomicU64,
}

impl EventTable {
    pub fn new(db: Arc<Db>) -> Self {
        Self {
            db,
            // XXX: Is there any real performance problem if we just implement
            // autoincrement on top of rocksdb?
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
        let key = EventKey::new(id);
        txn.put(&key, &event)?;

        // Index by tag + idempotency key
        let key = EventIdempotencyKey::new(event.tag(), event.idempotency_key());
        txn.put(&key, &id.to_ne_bytes())?;

        // Index by tag + stream
        let key = join_slices(&[
            &[Prefix::EventStreamIndex as u8],
            event.tag().as_bytes(),
            event.stream().as_bytes(),
        ]);
        txn.put(&*key, &id.to_ne_bytes())?;

        Ok(id)
    }

    pub fn get_by_id(&self, id: u64) -> DynResult<Option<DbSlice<'_, Event>>> {
        let key = EventKey::new(id);
        // XXX: Is pinning the slice here a performance win? In which cases?
        let slice = match self.db.get_pinned(&key)? {
            Some(slice) => slice,
            None => return Ok(None),
        };
        unsafe { Ok(Some(DbSlice::new(slice))) }
    }

    pub fn get_id_by_idempotency_key(
        &self,
        tag: Uuid,
        idempotency_key: Uuid,
    ) -> DynResult<Option<u64>> {
        let key = EventIdempotencyKey::new(tag, idempotency_key);
        let slice = match self.db.get(&key)? {
            Some(slice) => slice,
            None => return Ok(None),
        };
        let id = u64::from_ne_bytes(<[u8; 8]>::try_from(&slice[..]).unwrap());
        Ok(Some(id))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::testing::TestHarness;

    use super::{ContentType, EventBuilder, EventTable};

    #[test]
    fn test_builder() {
        let stream = "my_stream";
        let tag = uuid::uuid!("00000000-0000-8000-8000-000000000000");
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

        let events = EventTable::new(Arc::clone(&db));
        let txn = db.transaction();
        let tag = uuid::uuid!("00000000-0000-8000-8000-000000000000");
        let idempotency_key = uuid::uuid!("00000000-0000-8000-8000-000000000001");
        let event = EventBuilder::new()
            .content_type(ContentType::Json)
            .tag(tag)
            .stream("asdf")
            .payload(b"1234321")
            .idempotency_key(idempotency_key)
            .build();
        events.create(&txn, &event).unwrap();
        txn.commit().unwrap();

        let id = events
            .get_id_by_idempotency_key(tag, idempotency_key)
            .unwrap()
            .unwrap();
        assert_eq!(id, 0);

        let event2 = events.get_by_id(id).unwrap().unwrap();
        assert_eq!(&*event, &*event2);
    }
}
