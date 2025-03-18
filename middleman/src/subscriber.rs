use std::sync::Arc;

use db::bytes::AsBytes;
use db::key::{packed, Packed2};
use db::model::big_tuple_struct;
use db::{Accessor, ColumnFamily, Cursor, Db, Transaction};
use middleman_db::{self as db, ColumnFamilyDescriptor};
use middleman_macros::{OwnedFromBytesUnchecked, ToOwned};
use regex::Regex;
use url::Url;
use uuid::Uuid;

use crate::db::ColumnFamilyName;
use crate::error::Result;

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
        pub destination_url[1]: str,
        pub stream_regex[2]: str,
        pub hmac_key[3]: str,
    }
}

#[derive(Debug)]
pub(crate) struct SubscriberTable {
    cf: ColumnFamily,
    tag_index_cf: ColumnFamily,
}

pub(crate) type SubscriberKey = Uuid;
pub(crate) type TagIndexKey = Packed2<Uuid, Uuid>;

impl Subscriber {
    pub fn tag(&self) -> Uuid {
        self.header().tag
    }

    pub fn id(&self) -> Uuid {
        self.header().id
    }
}

impl SubscriberTable {
    pub(crate) fn new(db: Arc<Db>) -> Result<Self> {
        let cf = db.get_column_family(ColumnFamilyName::Subscribers.name()).unwrap();
        let tag_index_cf =
            db.get_column_family(ColumnFamilyName::SubscriberTagIndex.name()).unwrap();
        Ok(Self { cf, tag_index_cf })
    }

    fn accessor<'a>(&'a self) -> Accessor<'a, SubscriberKey, Subscriber> {
        Accessor::new(&self.cf)
    }

    fn tag_index_accessor<'a>(&'a self) -> Accessor<'a, TagIndexKey, ()> {
        Accessor::new(&self.tag_index_cf)
    }

    pub fn create(&self, txn: &mut Transaction<'_>, subscriber: &Subscriber) -> Result<()> {
        let key = subscriber.id();
        self.accessor().put_txn(txn, &key, subscriber);
        let key = packed!(subscriber.tag(), subscriber.id());
        self.tag_index_accessor().put_txn(txn, &key, &());
        Ok(())
    }

    pub fn get(&self, id: Uuid) -> Result<Option<Box<Subscriber>>> {
        unsafe { Ok(self.accessor().get_unchecked(&id)?) }
    }

    /// Iterates over subscribers by tag.
    pub fn iter_by_tag<'a>(
        &'a self,
        tag: Uuid,
    ) -> impl Iterator<Item = Result<Box<Subscriber>>> + 'a {
        unsafe {
            self.tag_index_accessor()
                .cursor_unchecked()
                .prefix_iter::<[u8; 16]>(*tag.as_bytes())
                .keys()
                .map(|key| {
                    let Packed2(_, id) = key?;
                    Ok(self.get(id).transpose().unwrap()?)
                })
        }
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

    pub fn build(&mut self) -> Result<Box<Subscriber>> {
        let destination_url = self.destination_url.take().ok_or("missing subscriber URL")?;
        if destination_url.scheme() != "http" && destination_url.scheme() != "https" {
            return Err("Invalid subscriber URL".into());
        }
        let stream_regex = self.stream_regex.take().ok_or("missing subscriber regex")?;
        let hmac_key = self.hmac_key.take().ok_or("missing subscriber HMAC key")?;
        let header = SubscriberHeader {
            tag: self.tag.ok_or("missing tag")?,
            id: self.id.ok_or("missing ID")?,
            _reserved: [0; 2],
        };
        Ok(Subscriber::new(
            &header,
            destination_url.as_str(),
            stream_regex.as_str(),
            hmac_key.as_str(),
        ))
    }
}

#[cfg(test)]
mod tests {
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
        let subscriber = SubscriberBuilder::new()
            .tag(tag)
            .destination_url(Url::parse(url).unwrap())
            .stream_regex(Regex::new(regex).unwrap())
            .id(id)
            .hmac_key("key".to_owned())
            .build()
            .unwrap();
        assert_eq!(subscriber.tag(), tag);
        assert_eq!(subscriber.stream_regex(), regex);
        assert_eq!(subscriber.destination_url(), url);

        let mut harness = TestHarness::new();
        let app = harness.application();
        let subscribers = &app.subscribers;

        let mut txn = Transaction::new(&app.db);
        subscribers.create(&mut txn, &subscriber).unwrap();
        txn.commit().unwrap();

        assert_eq!(subscribers.get(id).unwrap().unwrap(), subscriber);
    }
}
