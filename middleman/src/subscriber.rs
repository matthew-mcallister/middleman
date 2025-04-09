use std::sync::Arc;

use bytecast::{FromBytes, IntoBytes};
use bytecast_derive::{FromBytes, HasLayout, IntoBytes};
use db::key::{packed, Packed2};
use db::model::big_tuple_struct;
use db::{Accessor, ColumnFamily, Cursor, Db, Transaction};
use middleman_db::{self as db, ColumnFamilyDescriptor};
use regex::Regex;
use serde::ser::SerializeStruct;
use serde::Serialize;
use url::Url;
use uuid::Uuid;

use crate::api::to_json::{ConsumerApiSerializer, ProducerApiSerializer};
use crate::db::ColumnFamilyName;
use crate::error::Result;

#[derive(Debug, Clone, Copy, Eq, PartialEq, FromBytes, HasLayout, IntoBytes)]
#[repr(C)]
struct SubscriberHeader {
    tag: [u8; 16],
    id: [u8; 16],
    _reserved: [u64; 2],
}

big_tuple_struct! {
    /// Subscriber that receives events or notifications.
    pub struct Subscriber {
        header[0]: SubscriberHeader,
        pub destination_url_bytes[1]: [u8],
        pub stream_regex_bytes[2]: [u8],
        pub hmac_key_bytes[3]: [u8],
    }
}

impl Serialize for ProducerApiSerializer<Subscriber> {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut s = serializer.serialize_struct("Subscriber", 4)?;
        s.serialize_field("tag", &self.0.tag())?;
        s.serialize_field("id", &self.0.id())?;
        s.serialize_field("destination_url", &self.0.destination_url())?;
        s.serialize_field("stream_regex", &self.0.stream_regex())?;
        s.end()
    }
}

impl Serialize for ConsumerApiSerializer<Subscriber> {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut s = serializer.serialize_struct("Subscriber", 3)?;
        s.serialize_field("id", &self.0.id())?;
        s.serialize_field("destination_url", &self.0.destination_url())?;
        s.serialize_field("stream_regex", &self.0.stream_regex())?;
        s.end()
    }
}

#[derive(Debug)]
pub(crate) struct SubscriberTable {
    cf: ColumnFamily,
    tag_index_cf: ColumnFamily,
}

pub(crate) type SubscriberKey = [u8; 16];
pub(crate) type TagIndexKey = Packed2<[u8; 16], [u8; 16]>;

impl Subscriber {
    pub fn tag(&self) -> Uuid {
        Uuid::from_bytes(self.header().tag)
    }

    pub fn id(&self) -> Uuid {
        Uuid::from_bytes(self.header().id)
    }

    pub fn destination_url(&self) -> &str {
        std::str::from_utf8(self.destination_url_bytes()).unwrap()
    }

    pub fn stream_regex(&self) -> &str {
        std::str::from_utf8(self.stream_regex_bytes()).unwrap()
    }

    pub fn hmac_key(&self) -> &str {
        std::str::from_utf8(self.hmac_key_bytes()).unwrap()
    }
}

impl SubscriberTable {
    pub(crate) fn new(db: Arc<Db>) -> Result<Self> {
        let cf = db.get_column_family(ColumnFamilyName::Subscribers.name()).unwrap();
        let tag_index_cf =
            db.get_column_family(ColumnFamilyName::SubscriberTagIndex.name()).unwrap();
        Ok(Self { cf, tag_index_cf })
    }

    pub(crate) fn db(&self) -> &Arc<Db> {
        self.cf.db()
    }

    fn accessor<'a>(&'a self) -> Accessor<'a, SubscriberKey, Subscriber> {
        Accessor::new(&self.cf)
    }

    fn tag_index_accessor<'a>(&'a self) -> Accessor<'a, TagIndexKey, ()> {
        Accessor::new(&self.tag_index_cf)
    }

    pub fn create(
        &self,
        txn: &mut Transaction,
        mut builder: SubscriberBuilder,
    ) -> Result<Box<Subscriber>> {
        let subscriber = builder.build()?;
        self.accessor().put_txn(txn, subscriber.id().as_bytes(), &subscriber);
        let key = packed!(subscriber.tag().into_bytes(), subscriber.id().into_bytes());
        self.tag_index_accessor().put_txn(txn, &key, &());
        Ok(subscriber)
    }

    pub fn get(&self, id: Uuid) -> Result<Option<Box<Subscriber>>> {
        Ok(self.accessor().get(id.as_bytes())?)
    }

    pub fn iter<'a>(&'a self) -> impl Iterator<Item = Result<Box<Subscriber>>> + 'a {
        let mut cursor = self.accessor().cursor();
        cursor.seek_to_first();
        cursor.values().map(|r| r.map_err(Into::into))
    }

    /// Iterates over subscribers by tag.
    pub fn iter_by_tag<'a>(
        &'a self,
        tag: Uuid,
    ) -> impl Iterator<Item = Result<Box<Subscriber>>> + 'a {
        self.tag_index_accessor().cursor().prefix_iter::<[u8; 16]>(*tag.as_bytes()).keys().map(
            |key| {
                let Packed2(_, id) = key?;
                Ok(self.get(Uuid::from_bytes(id)).transpose().unwrap()?)
            },
        )
    }

    /// Iterates over all subscribers of the given stream. This may be slow if
    /// there are a lot of subscribers for a given tag.
    pub fn iter_by_stream<'a>(
        &'a self,
        tag: Uuid,
        stream: &'a str,
    ) -> impl Iterator<Item = Result<Box<Subscriber>>> + 'a {
        self.iter_by_tag(tag).filter_map(|item| {
            let Ok(subscriber) = item else { return Some(item.map_err(Into::into)) };
            let regex = Regex::new(subscriber.stream_regex()).unwrap();
            if regex.is_match(stream) {
                Some(Ok(subscriber))
            } else {
                None
            }
        })
    }
}

#[derive(Debug, Default)]
pub struct SubscriberBuilder {
    tag: Option<Uuid>,
    id: Option<Uuid>,
    destination_url: Option<Url>,
    stream_regex: Option<Regex>,
    hmac_key: Option<String>,
    max_connections: Option<u16>,
}

impl SubscriberBuilder {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn tag(&mut self, tag: Uuid) -> &mut Self {
        self.tag = Some(tag);
        self
    }

    pub fn id(&mut self, id: Uuid) -> &mut Self {
        self.id = Some(id);
        self
    }

    pub fn destination_url(&mut self, destination_url: Url) -> &mut Self {
        self.destination_url = Some(destination_url);
        self
    }

    pub fn stream_regex(&mut self, regex: Regex) -> &mut Self {
        self.stream_regex = Some(regex);
        self
    }

    pub fn hmac_key(&mut self, key: String) -> &mut Self {
        self.hmac_key = Some(key);
        self
    }

    pub fn max_connections(&mut self, max_connections: u16) -> &mut Self {
        // TODO: Actually handle this field
        self.max_connections = Some(max_connections);
        self
    }

    fn build(&mut self) -> Result<Box<Subscriber>> {
        let destination_url = self.destination_url.take().ok_or("missing subscriber URL")?;
        if destination_url.scheme() != "http" && destination_url.scheme() != "https" {
            return Err("Invalid subscriber URL".into());
        }
        let stream_regex = self.stream_regex.take().ok_or("missing subscriber regex")?;
        let hmac_key = self.hmac_key.take().ok_or("missing subscriber HMAC key")?;
        let header = SubscriberHeader {
            tag: self.tag.ok_or("missing tag")?.into_bytes(),
            id: self.id.ok_or("missing ID")?.into_bytes(),
            _reserved: [0; 2],
        };
        Ok(Subscriber::new(
            &header,
            destination_url.as_str().as_bytes(),
            stream_regex.as_str().as_bytes(),
            hmac_key.as_str().as_bytes(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use regex::Regex;
    use url::Url;

    use crate::testing::TestHarness;
    use db::Transaction;
    use middleman_db as db;

    use super::SubscriberBuilder;

    #[test]
    fn test_create_subscriber() {
        let url = "https://example.com/webhook";
        let regex = "^hello";
        let tag = uuid::uuid!("00000000-0000-8000-8000-000000000000");
        let id = uuid::uuid!("00000000-0000-8000-8000-000000000001");
        let mut builder = SubscriberBuilder::new();
        builder
            .id(id)
            .tag(tag)
            .destination_url(Url::parse(url).unwrap())
            .stream_regex(Regex::new(regex).unwrap())
            .hmac_key("key".to_owned());

        let mut harness = TestHarness::new();
        let app = harness.application();
        let subscribers = &app.subscribers;

        let mut txn = Transaction::new(Arc::clone(&app.db));
        let subscriber = subscribers.create(&mut txn, builder).unwrap();
        txn.commit().unwrap();

        assert_eq!(subscriber.tag(), tag);
        assert_eq!(subscriber.stream_regex(), regex);
        assert_eq!(subscriber.destination_url(), url);

        assert_eq!(subscribers.get(subscriber.id()).unwrap().unwrap(), subscriber,);
    }
}
