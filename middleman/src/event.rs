use std::borrow::Cow;
use std::fmt::Display;
use std::sync::Arc;

use bytecast::IntoBytes;
use bytecast_derive::{FromBytes, HasLayout, IntoBytes};
use db::big_tuple::{big_tuple, BigTuple};
use db::key::{packed, BigEndianU64, Packed2, Packed3};
use db::model::big_tuple_struct;
use db::{Accessor, ColumnFamily, Cursor, Db, Transaction};
use middleman_db::{self as db, ColumnFamilyDescriptor, Sequence};
use uuid::Uuid;

use crate::api::to_json::{ConsumerApiSerializer, JsonFormatter, ProducerApiSerializer};
use crate::db::ColumnFamilyName;
use crate::error::Result;

// TODO: ID should probably be stored on the event
#[derive(Clone, Copy, Debug, Eq, FromBytes, HasLayout, IntoBytes, PartialEq)]
#[repr(C)]
struct EventHeader {
    idempotency_key: [u8; 16],
    tag: [u8; 16],
    id: u64,
    _reserved: [u64; 2],
}

big_tuple_struct! {
    /// Immutable event data structure.
    pub struct Event {
        header[0]: EventHeader,
        pub stream_bytes[1]: [u8],
        pub payload_bytes[2]: [u8],
    }
}

impl Event {
    pub fn id(&self) -> u64 {
        self.header().id
    }

    pub fn idempotency_key(&self) -> Uuid {
        Uuid::from_bytes(self.header().idempotency_key)
    }

    pub fn tag(&self) -> Uuid {
        Uuid::from_bytes(self.header().tag)
    }

    pub fn stream(&self) -> &str {
        std::str::from_utf8(self.stream_bytes()).unwrap()
    }

    pub fn payload(&self) -> &str {
        std::str::from_utf8(self.payload_bytes()).unwrap()
    }
}

impl Display for JsonFormatter<ProducerApiSerializer<Event>> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            concat!(
                "{{",
                "\"id\":{id}",
                ",\"idempotency_key\":\"{idempotency_key}\"",
                ",\"tag\":\"{tag}\"",
                ",\"stream\":\"{stream}\"",
                ",\"payload\":{payload}",
                "}}",
            ),
            id = self.0 .0.id(),
            idempotency_key = self.0 .0.idempotency_key(),
            tag = self.0 .0.tag(),
            stream = self.0 .0.stream(),
            // TODO maybe: the string cast here has a small but nontrivial
            // overhead, esp. if the payload is large (100kb+). Could avoid by
            // treating as bytes instead of str.
            payload = self.0 .0.payload(),
        )
    }
}

impl Display for JsonFormatter<ConsumerApiSerializer<Event>> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            concat!(
                "{{",
                "\"id\":{id}",
                ",\"stream\":\"{stream}\"",
                ",\"payload\":{payload}",
                "}}",
            ),
            id = self.0 .0.id(),
            stream = self.0 .0.stream(),
            payload = self.0 .0.payload(),
        )
    }
}

#[derive(Clone, Debug, Default)]
pub struct EventBuilder<'a> {
    idempotency_key: Option<Uuid>,
    tag: Option<Uuid>,
    stream: Option<Cow<'a, str>>,
    payload: Option<Cow<'a, str>>,
}

impl<'a> EventBuilder<'a> {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn idempotency_key(&mut self, key: impl Into<Uuid>) -> &mut Self {
        self.idempotency_key = Some(key.into());
        self
    }

    pub fn tag(&mut self, tag: Uuid) -> &mut Self {
        self.tag = Some(tag);
        self
    }

    pub fn stream(&mut self, stream: impl Into<Cow<'a, str>>) -> &mut Self {
        self.stream = Some(stream.into());
        self
    }

    pub fn payload(&mut self, payload: impl Into<Cow<'a, str>>) -> &mut Self {
        self.payload = Some(payload.into());
        self
    }

    pub(crate) fn build(&mut self, id: u64) -> Box<Event> {
        Event::new(
            &EventHeader {
                tag: self.tag.unwrap().into_bytes(),
                idempotency_key: self.idempotency_key.unwrap().into_bytes(),
                id,
                _reserved: [0; 2],
            },
            &*self.stream.take().unwrap().as_bytes(),
            &*self.payload.take().unwrap().as_bytes(),
        )
    }
}

#[derive(Debug)]
pub(crate) struct EventTable {
    id_sequence: db::Sequence,
    cf: ColumnFamily,
    tag_idempotency_index_cf: ColumnFamily,
    tag_stream_index_cf: ColumnFamily,
}

big_tuple_struct! {
    pub struct EventStreamIndexKey {
        tag[0]: [u8; 16],
        stream[1]: [u8],
        id[2]: BigEndianU64,
    }
}

impl EventTable {
    pub fn new(db: Arc<Db>) -> Result<Self> {
        let cf = db.get_column_family(ColumnFamilyName::Events.name()).unwrap();
        let tag_idempotency_index_cf =
            db.get_column_family(ColumnFamilyName::EventTagIdempotencyKeyIndex.name()).unwrap();
        let tag_stream_index_cf =
            db.get_column_family(ColumnFamilyName::EventTagStreamIndex.name()).unwrap();
        let meta_cf = db.get_column_family(ColumnFamilyName::Meta.name()).unwrap();
        Ok(Self {
            id_sequence: Sequence::new(meta_cf, "event_id")?,
            cf,
            tag_idempotency_index_cf,
            tag_stream_index_cf,
        })
    }

    fn accessor<'a>(&'a self) -> Accessor<'a, Packed2<[u8; 16], BigEndianU64>, Event> {
        Accessor::new(&self.cf)
    }

    fn tag_idempotency_index_accessor<'a>(
        &'a self,
    ) -> Accessor<'a, Packed2<[u8; 16], [u8; 16]>, u64> {
        Accessor::new(&self.tag_idempotency_index_cf)
    }

    fn tag_stream_index_accessor<'a>(&'a self) -> Accessor<'a, EventStreamIndexKey, ()> {
        Accessor::new(&self.tag_stream_index_cf)
    }

    /// Creates an event. Event creation is atomic but *not* synchronized,
    /// meaning that events from different sources or partitions are not
    /// guaranteed to be ordered.
    // XXX: Is it possible to catch write conflicts and report those as a
    // unique status so the operation need not be retried?
    pub fn create(&self, txn: &mut Transaction, mut builder: EventBuilder) -> Result<Box<Event>> {
        let (tag, idempotency_key) = (builder.tag.unwrap(), builder.idempotency_key.unwrap());

        // Acquire lock
        let key: Packed3<[u8; 6], [u8; 16], [u8; 16]> =
            (*b"event:", tag.into_bytes(), idempotency_key.into_bytes()).into();
        txn.lock_key(&self.cf, IntoBytes::as_bytes(&key))?;

        // Try to fetch existing record by idempotency key
        let existing_id = self.get_id_by_idempotency_key(txn, tag, idempotency_key)?;
        if let Some(id) = existing_id {
            return Ok(builder.build(id));
        }

        // Create primary record
        let id = self.id_sequence.next()?;
        let event = builder.build(id);
        self.accessor().put_txn(txn, &packed!(tag.into_bytes(), id.into()), &event);

        // Index by tag + idempotency key
        let key = packed!(tag.into_bytes(), idempotency_key.into_bytes());
        self.tag_idempotency_index_accessor().put_txn(txn, &key, &id);

        // Index by tag + stream
        let key = EventStreamIndexKey::new(tag.as_bytes(), event.stream_bytes(), &id.into());
        self.tag_stream_index_accessor().put_txn(txn, &key, &());

        Ok(event)
    }

    pub fn get(&self, tag: Uuid, id: u64) -> Result<Option<Box<Event>>> {
        // XXX: Is pinning the slice here a performance win? In which cases?
        let key = packed!(tag.into_bytes(), id.into());
        Ok(self.accessor().get(&key)?)
    }

    pub fn get_id_by_idempotency_key(
        &self,
        txn: &mut Transaction,
        tag: Uuid,
        idempotency_key: Uuid,
    ) -> Result<Option<u64>> {
        let key = packed!(tag.into_bytes(), idempotency_key.into_bytes());
        self.tag_idempotency_index_accessor()
            .get_txn(txn, &key)
            .map(|x| x.map(|x| *x))
            .map_err(Into::into)
    }

    pub fn get_by_idempotency_key(
        &self,
        txn: &mut Transaction,
        tag: Uuid,
        idempotency_key: Uuid,
    ) -> Result<Option<Box<Event>>> {
        let id = self.get_id_by_idempotency_key(txn, tag, idempotency_key)?;
        let Some(id) = id else { return Ok(None) };
        let Some(event) = self.get(tag, id)? else { return Ok(None) };
        Ok(Some(event))
    }

    pub fn iter_by_tag<'a>(
        &'a self,
        tag: Uuid,
        starting_id: u64,
    ) -> impl Iterator<Item = Result<Box<Event>>> + 'a {
        let mut cursor = self.accessor().cursor();
        cursor.seek(&packed!(tag.into_bytes(), starting_id.into()));
        cursor.prefix::<[u8; 16]>(*tag.as_bytes()).values().map(|x| x.map_err(Into::into))
    }

    pub fn iter_by_stream<'a>(
        &'a self,
        tag: Uuid,
        stream: &str,
        starting_id: u64,
    ) -> impl Iterator<Item = Result<Box<Event>>> + 'a {
        let prefix = big_tuple!(tag.as_bytes(), stream.as_bytes());
        let mut cursor = self.tag_stream_index_accessor().cursor();
        let seek_to =
            EventStreamIndexKey::new(tag.as_bytes(), stream.as_bytes(), &starting_id.into());
        cursor.seek(&seek_to);
        cursor.prefix::<BigTuple>(prefix).keys().map(move |key| {
            let id = *key?.id();
            // XXX: These access are monotonic and should be highly coherent.
            // Is there a way to make them faster?
            let event = self.get(tag, id.into())?.unwrap();
            Ok(event)
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use db::transaction::Transaction;
    use middleman_db as db;

    use super::EventBuilder;
    use crate::error::ErrorKind;
    use crate::testing::TestHarness;

    #[test]
    fn test_builder() {
        let stream = "my_stream";
        let tag = uuid::uuid!("00000000-0000-8000-8000-000000000000");
        let idempotency_key = uuid::uuid!("00000000-0000-8000-8000-000000000001");
        let payload = "1234ideclareathumbwar";
        let event = EventBuilder::new()
            .idempotency_key(idempotency_key)
            .stream(stream)
            .tag(tag)
            .payload(payload)
            .build(0);
        assert_eq!(event.stream(), stream);
        assert_eq!(event.tag(), tag);
        assert_eq!(event.idempotency_key(), idempotency_key);
        assert_eq!(event.payload(), payload);
    }

    fn testing_event() -> EventBuilder<'static> {
        let mut builder = EventBuilder::new();
        builder
            .tag(uuid::uuid!("00000000-0000-8000-8000-000000000000"))
            .stream("asdf")
            .payload("1234321")
            .idempotency_key(uuid::uuid!("00000000-0000-8000-8000-000000000001"));
        builder
    }

    #[test]
    fn test_create() {
        let mut harness = TestHarness::new();
        let app = harness.application();
        let events = &app.events;

        let event = testing_event();
        let mut txn = Transaction::new(Arc::clone(&app.db));
        let event = events.create(&mut txn, event).unwrap();
        txn.commit().unwrap();

        let event2 = events.get(event.tag(), event.id()).unwrap().unwrap();
        assert_eq!(&*event, &*event2);
    }

    /// When the same event is committed in two (non-overlapping) transactions,
    /// the second transaction will find and return the existing event.
    #[test]
    fn test_idempotency() {
        let mut harness = TestHarness::new();
        let app = harness.application();
        let events = &app.events;

        let builder = testing_event();
        let mut txn = Transaction::new(Arc::clone(&app.db));
        let event = events.create(&mut txn, builder.clone()).unwrap();
        txn.commit().unwrap();

        let mut txn = Transaction::new(Arc::clone(&app.db));
        let event2 = events.create(&mut txn, builder).unwrap();
        txn.commit().unwrap();
        assert_eq!(event, event2);

        // Also test lookup by idempotency key
        let mut txn = Transaction::new(Arc::clone(&app.db));
        let event2 = events
            .get_by_idempotency_key(&mut txn, event.tag(), event.idempotency_key())
            .unwrap()
            .unwrap();
        assert_eq!(event, event2);
        txn.commit().unwrap();
    }

    #[test]
    fn test_iter_by_stream() {
        let mut harness = TestHarness::new();
        let app = harness.application();
        let events = &app.events;

        let mut txn = Transaction::new(Arc::clone(&app.db));
        let mut base = EventBuilder::new();
        let tag = uuid::uuid!("00000000-0000-8000-8000-000000000000");
        base.tag(tag);
        let mut event1 = base.clone();
        event1
            .stream("stream1")
            .payload("1")
            .idempotency_key(uuid::uuid!("00000000-0000-8000-8000-000000000001"));
        let event1 = events.create(&mut txn, event1).unwrap();
        let mut event2 = base.clone();
        event2
            .stream("stream0")
            .payload("2")
            .idempotency_key(uuid::uuid!("00000000-0000-8000-8000-000000000002"));
        let _event2 = events.create(&mut txn, event2).unwrap();
        let mut event3 = base.clone();
        event3
            .stream("stream1")
            .payload("3")
            .idempotency_key(uuid::uuid!("00000000-0000-8000-8000-000000000003"));
        let event3 = events.create(&mut txn, event3).unwrap();
        let mut event4 = base.clone();
        event4
            .stream("strm2")
            .payload("4")
            .idempotency_key(uuid::uuid!("00000000-0000-8000-8000-000000000004"));
        let _event4 = events.create(&mut txn, event4).unwrap();
        txn.commit().unwrap();

        let mut iter = events.iter_by_stream(tag, "stream1", 0);
        assert_eq!(iter.next().unwrap().unwrap(), event1);
        assert_eq!(iter.next().unwrap().unwrap(), event3);
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_lock_race() {
        let mut harness = TestHarness::new();
        let app = harness.application();
        let events = &app.events;

        let event = testing_event();
        let mut txn1 = Transaction::new(Arc::clone(&app.db));
        events.create(&mut txn1, event.clone()).unwrap();

        let mut txn2 = Transaction::new(Arc::clone(&app.db));
        assert_eq!(events.create(&mut txn2, event).unwrap_err().kind(), ErrorKind::Busy);
    }
}
