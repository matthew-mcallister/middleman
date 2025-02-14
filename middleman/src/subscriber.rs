use std::sync::Arc;

use middleman_macros::{OwnedFromBytesUnchecked, ToOwned};
use regex::Regex;
use url::Url;
use uuid::Uuid;

use crate::accessor::CfAccessor;
use crate::bytes::AsBytes;
use crate::error::DynResult;
use crate::key::{packed, Packed2};
use crate::model::big_tuple_struct;
use crate::types::{Db, DbColumnFamily, DbTransaction};
use crate::util::get_cf;
use crate::ColumnFamilyName;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
struct SubscriberHeader {
    tag: Uuid,
    id: Uuid,
    _reserved: [u64; 2],
}

unsafe impl AsBytes for SubscriberHeader {}

big_tuple_struct! {
    /// Subscriber that receives events or notifications.
    pub struct Subscriber {
        header[0]: SubscriberHeader,
        destination_url[1]: str,
        stream_regex[2]: str,
    }
}

pub(crate) struct SubscriberTable {
    cf: DbColumnFamily,
    // NB: Drop last
    db: Arc<Db>,
}

pub(crate) type SubscriberKey = Packed2<Uuid, Uuid>;

impl Subscriber {
    pub fn tag(&self) -> Uuid {
        self.header().tag
    }

    pub fn id(&self) -> Uuid {
        self.header().id
    }
}

impl SubscriberTable {
    pub(crate) fn new(db: Arc<Db>) -> DynResult<Self> {
        let cf = unsafe { get_cf(&db, ColumnFamilyName::Subscribers) };
        Ok(Self { db, cf })
    }

    fn accessor<'a>(&'a self) -> CfAccessor<'a, SubscriberKey, Subscriber> {
        CfAccessor::new(&self.db, &self.cf)
    }

    pub fn create(&self, txn: &DbTransaction, subscriber: &Subscriber) -> DynResult<()> {
        let key = packed!(subscriber.tag(), subscriber.id());
        self.accessor().put_txn(txn, &key, subscriber)?;
        Ok(())
    }

    pub fn get(&self, tag: Uuid, id: Uuid) -> DynResult<Option<Box<Subscriber>>> {
        let key = packed!(tag, id);
        unsafe { self.accessor().get_unchecked(&key) }
    }

    /// Iterates over subscribers by tag.
    pub fn iter_by_tag<'txn>(
        &self,
        txn: &'txn DbTransaction<'txn>,
        tag: Uuid,
    ) -> impl Iterator<Item = DynResult<Box<Subscriber>>> + 'txn {
        unsafe {
            self.accessor()
                .iter_by_prefix_txn_unchecked::<[u8; 16]>(txn, *tag.as_bytes())
                .map(|e| e.map(|(_, v)| v))
        }
    }

    /// Iterates over all subscribers of the given stream. This may be slow if
    /// there are a lot of subscribers for a given tag.
    pub fn iter_by_stream<'a>(
        &self,
        txn: &'a DbTransaction<'a>,
        tag: Uuid,
        stream: &'a str,
    ) -> impl Iterator<Item = DynResult<Box<Subscriber>>> + 'a {
        self.iter_by_tag(txn, tag).filter_map(|item| {
            let Ok(subscriber) = item else { return Some(item) };
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

    pub fn build(&mut self) -> DynResult<Box<Subscriber>> {
        let destination_url = self.destination_url.take().ok_or("Missing subscriber URL")?;
        if destination_url.scheme() != "http" && destination_url.scheme() != "https" {
            return Err("Invalid subscriber URL".into());
        }
        let stream_regex = self.stream_regex.take().ok_or("Missing subscriber regex")?;
        let header = SubscriberHeader {
            tag: self.tag.ok_or("Missing tag")?,
            id: self.id.ok_or("Missing ID")?,
            _reserved: [0; 2],
        };
        Ok(Subscriber::new(
            &header,
            destination_url.as_str(),
            stream_regex.as_str(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use regex::Regex;
    use url::Url;

    use crate::subscriber::SubscriberTable;
    use crate::testing::TestHarness;

    use super::SubscriberBuilder;

    #[test]
    fn test_create_subscriber() {
        let url = "https://example.com/webhook";
        let regex = "^hello";
        let tag = uuid::uuid!("00000000-0000-8000-8000-000000000000");
        let id = uuid::uuid!("00000000-0000-8000-8000-000000000001");
        let subscriber = SubscriberBuilder::new()
            .tag(tag)
            .destination_url(Url::parse(url).unwrap())
            .stream_regex(Regex::new(regex).unwrap())
            .id(id)
            .build()
            .unwrap();
        assert_eq!(subscriber.tag(), tag);
        assert_eq!(subscriber.stream_regex(), regex);
        assert_eq!(subscriber.destination_url(), url);

        let mut harness = TestHarness::new();
        let app = harness.application();
        let db = &app.db;
        let subscribers = &app.subscribers;

        let txn = db.transaction();
        subscribers.create(&txn, &subscriber).unwrap();
        txn.commit().unwrap();

        assert_eq!(subscribers.get(tag, id).unwrap().unwrap(), subscriber);
    }
}
