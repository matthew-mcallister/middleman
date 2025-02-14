use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use middleman_macros::{OwnedFromBytesUnchecked, ToOwned};
use uuid::Uuid;

use crate::accessor::CfAccessor;
use crate::big_tuple::{big_tuple, BigTuple};
use crate::bytes::AsBytes;
use crate::error::DynResult;
use crate::key::{packed, BigEndianU64, Packed2};
use crate::model::big_tuple_struct;
use crate::types::{ContentType, Db, DbColumnFamily, DbTransaction};
use crate::util::get_cf;
use crate::ColumnFamilyName;

// TODO: ID should probably be stored on the event
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(C, align(8))]
struct EventHeader {
    idempotency_key: Uuid,
    tag: Uuid,
    _reserved: [u64; 2],
}

unsafe impl AsBytes for EventHeader {}

big_tuple_struct! {
    /// Immutable event data structure.
    pub struct Event {
        header[0]: EventHeader,
        pub stream[1]: str,
        pub payload[2]: [u8],
    }
}

impl Event {
    pub fn content_type(&self) -> ContentType {
        // TODO: Read content type from flags
        ContentType::Json
    }

    pub fn idempotency_key(&self) -> Uuid {
        self.header().idempotency_key
    }

    pub fn tag(&self) -> Uuid {
        self.header().tag
    }
}

#[derive(Clone, Debug, Default)]
pub struct EventBuilder<'a> {
    idempotency_key: Option<Uuid>,
    tag: Option<Uuid>,
    content_type: Option<ContentType>,
    stream: Option<&'a str>,
    payload: Option<&'a [u8]>,
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
        self.idempotency_key = Some(key.into());
        self
    }

    pub fn tag(&mut self, tag: Uuid) -> &mut Self {
        self.tag = Some(tag);
        self
    }

    pub fn stream(&mut self, stream: &'a str) -> &mut Self {
        self.stream = Some(stream);
        self
    }

    pub fn payload(&mut self, payload: &'a [u8]) -> &mut Self {
        self.payload = Some(payload);
        self
    }

    pub fn build(&mut self) -> Box<Event> {
        Event::new(
            &EventHeader {
                tag: self.tag.unwrap(),
                idempotency_key: self.idempotency_key.unwrap(),
                _reserved: [0; 2],
            },
            self.stream.unwrap(),
            self.payload.unwrap(),
        )
    }
}

pub(crate) struct EventTable {
    // XXX: Need a persistent autoincrement implementation
    event_sequence_number: AtomicU64,
    cf: DbColumnFamily,
    tag_idempotency_index_cf: DbColumnFamily,
    tag_stream_index_cf: DbColumnFamily,
    // NB: Drop last
    db: Arc<Db>,
}

big_tuple_struct! {
    pub struct EventStreamIndexKey {
        tag[0]: Uuid,
        stream[1]: str,
        id[2]: BigEndianU64,
    }
}

impl EventTable {
    pub fn new(db: Arc<Db>) -> DynResult<Self> {
        unsafe {
            let cf = get_cf(&db, ColumnFamilyName::Events);
            let tag_idempotency_index_cf =
                get_cf(&db, ColumnFamilyName::EventTagIdempotencyKeyIndex);
            let tag_stream_index_cf = get_cf(&db, ColumnFamilyName::EventTagStreamIndex);
            Ok(Self {
                db,
                event_sequence_number: AtomicU64::new(0),
                cf,
                tag_idempotency_index_cf,
                tag_stream_index_cf,
            })
        }
    }

    fn accessor<'a>(&'a self) -> CfAccessor<'a, BigEndianU64, Event> {
        CfAccessor::new(&self.db, &self.cf)
    }

    fn tag_idempotency_index_accessor<'a>(&'a self) -> CfAccessor<'a, Packed2<Uuid, Uuid>, u64> {
        CfAccessor::new(&self.db, &self.tag_idempotency_index_cf)
    }

    fn tag_stream_index_accessor<'a>(&'a self) -> CfAccessor<'a, EventStreamIndexKey, ()> {
        CfAccessor::new(&self.db, &self.tag_stream_index_cf)
    }

    /// Creates an event. Event creation is atomic but *not* synchronized,
    /// meaning that events from different sources or partitions are not
    /// guaranteed to be ordered.
    // XXX: Is it possible to catch write conflicts and report those as a
    // unique status so the operation need not be retried?
    pub fn create(&self, txn: &DbTransaction<'_>, event: &Event) -> DynResult<u64> {
        let (tag, idempotency_key) = (event.tag(), event.idempotency_key());

        // Try to fetch existing record by idempotency key
        let existing_id = self.get_id_by_idempotency_key(txn, tag, idempotency_key)?;
        if let Some(id) = existing_id {
            return Ok(id);
        }

        // Create primary record
        let id = self.event_sequence_number.fetch_add(1, Ordering::Relaxed);
        self.accessor().put_txn(txn, &id.into(), &event)?;

        // Index by tag + idempotency key
        let key = packed!(tag, idempotency_key);
        self.tag_idempotency_index_accessor().put_txn(txn, &key, &id)?;

        // Index by tag + stream
        let key = EventStreamIndexKey::new(&tag, event.stream(), &id.into());
        self.tag_stream_index_accessor().put_txn(txn, &key, &())?;

        Ok(id)
    }

    pub fn get(&self, id: u64) -> DynResult<Option<Box<Event>>> {
        // XXX: Is pinning the slice here a performance win? In which cases?
        unsafe { self.accessor().get_unchecked(&id.into()) }
    }

    pub fn get_id_by_idempotency_key(
        &self,
        txn: &DbTransaction,
        tag: Uuid,
        idempotency_key: Uuid,
    ) -> DynResult<Option<u64>> {
        let key: Packed2<Uuid, Uuid> = (tag, idempotency_key).into();
        unsafe {
            self.tag_idempotency_index_accessor()
                .get_txn_unchecked(txn, &key)
                .map(|x| x.map(|x| *x))
        }
    }

    pub fn get_by_idempotency_key(
        &self,
        txn: &DbTransaction,
        tag: Uuid,
        idempotency_key: Uuid,
    ) -> DynResult<Option<(u64, Box<Event>)>> {
        let id = self.get_id_by_idempotency_key(txn, tag, idempotency_key)?;
        let Some(id) = id else { return Ok(None) };
        let Some(event) = self.get(id)? else { return Ok(None) };
        Ok(Some((id, event)))
    }

    pub fn iter_by_stream<'a>(
        &'a self,
        tag: Uuid,
        stream: &str,
    ) -> impl Iterator<Item = DynResult<(u64, Box<Event>)>> + 'a {
        let prefix = big_tuple!(&tag, stream);
        unsafe {
            self.tag_stream_index_accessor().iter_keys_by_prefix_unchecked::<BigTuple>(prefix).map(
                move |key| {
                    let id = *key?.id();
                    let event = self.accessor().get_unchecked(&id)?.unwrap();
                    Ok((id.into(), event))
                },
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::testing::TestHarness;

    use super::{ContentType, Event, EventBuilder, EventTable};

    #[test]
    fn test_builder() {
        let stream = "my_stream";
        let tag = uuid::uuid!("00000000-0000-8000-8000-000000000000");
        let idempotency_key = uuid::uuid!("00000000-0000-8000-8000-000000000001");
        let payload = b"1234ideclareathumbwar";
        let event = EventBuilder::new()
            .content_type(ContentType::Json)
            .idempotency_key(idempotency_key)
            .stream(stream)
            .tag(tag)
            .payload(payload)
            .build();
        assert_eq!(event.content_type(), ContentType::Json);
        assert_eq!(event.stream(), stream);
        assert_eq!(event.tag(), tag);
        assert_eq!(event.idempotency_key(), idempotency_key);
        assert_eq!(event.payload(), payload);
    }

    fn testing_event() -> Box<Event> {
        EventBuilder::new()
            .content_type(ContentType::Json)
            .tag(uuid::uuid!("00000000-0000-8000-8000-000000000000"))
            .stream("asdf")
            .payload(b"1234321")
            .idempotency_key(uuid::uuid!("00000000-0000-8000-8000-000000000001"))
            .build()
    }

    #[test]
    fn test_create() {
        let mut harness = TestHarness::new();
        let app = harness.application();
        let db = &app.db;
        let events = &app.events;

        let event = testing_event();
        let txn = db.transaction();
        let id = events.create(&txn, &event).unwrap();
        txn.commit().unwrap();

        let event2 = events.get(id).unwrap().unwrap();
        assert_eq!(&*event, &*event2);
    }

    /// When the same event is committed in two (non-overlapping) transactions,
    /// the second transaction will find and return the existing event.
    #[test]
    fn test_idempotency() {
        let mut harness = TestHarness::new();
        let app = harness.application();
        let db = &app.db;
        let events = &app.events;

        let event = testing_event();
        let txn = db.transaction();
        let id = events.create(&txn, &event).unwrap();
        txn.commit().unwrap();

        let txn = db.transaction();
        let id2 = events.create(&txn, &event).unwrap();
        txn.commit().unwrap();
        assert_eq!(id, id2);

        // Also test lookup by idempotency key
        let txn = db.transaction();
        let (id2, event2) = events
            .get_by_idempotency_key(&txn, event.tag(), event.idempotency_key())
            .unwrap()
            .unwrap();
        assert_eq!(id, id2);
        assert_eq!(event, event2);
        txn.commit().unwrap();
    }

    /// When the same event is committed in two (snapshot) transactions,
    /// one of the transactions will fail.
    #[test]
    #[should_panic]
    fn test_idempotency_conflict() {
        let mut harness = TestHarness::new();
        let app = harness.application();
        let db = &app.db;
        let events = &app.events;

        let event = testing_event();
        let mut opts = rocksdb::OptimisticTransactionOptions::new();
        opts.set_snapshot(true);

        let txn1 = db.transaction_opt(&Default::default(), &opts);
        let txn2 = db.transaction_opt(&Default::default(), &opts);
        events.create(&txn1, &event).unwrap();
        events.create(&txn2, &event).unwrap();
        txn1.commit().unwrap();
        txn2.commit().unwrap();
    }

    #[test]
    fn test_iter_by_stream() {
        let mut harness = TestHarness::new();
        let app = harness.application();
        let db = &app.db;
        let events = &app.events;

        let txn = db.transaction();
        let mut base = EventBuilder::new();
        let tag = uuid::uuid!("00000000-0000-8000-8000-000000000000");
        base.content_type(ContentType::Json).tag(tag);
        let event1 = base
            .clone()
            .stream("stream1")
            .payload(b"1")
            .idempotency_key(uuid::uuid!("00000000-0000-8000-8000-000000000001"))
            .build();
        let id1 = events.create(&txn, &event1).unwrap();
        let event2 = base
            .clone()
            .stream("stream0")
            .payload(b"2")
            .idempotency_key(uuid::uuid!("00000000-0000-8000-8000-000000000002"))
            .build();
        let _id2 = events.create(&txn, &event2).unwrap();
        let event3 = base
            .clone()
            .stream("stream1")
            .payload(b"3")
            .idempotency_key(uuid::uuid!("00000000-0000-8000-8000-000000000003"))
            .build();
        let id3 = events.create(&txn, &event3).unwrap();
        let event4 = base
            .clone()
            .stream("strm2")
            .payload(b"4")
            .idempotency_key(uuid::uuid!("00000000-0000-8000-8000-000000000004"))
            .build();
        let _id4 = events.create(&txn, &event4).unwrap();
        txn.commit().unwrap();

        let mut iter = events.iter_by_stream(tag, "stream1");
        assert_eq!(iter.next().unwrap().unwrap(), (id1, event1));
        assert_eq!(iter.next().unwrap().unwrap(), (id3, event3));
        assert!(iter.next().is_none());
    }
}
