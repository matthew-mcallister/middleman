use regex::Regex;
use url::Url;
use uuid::Uuid;

use crate::delivery::Delivery;
use crate::error::DynResult;
use crate::event::Event;
use crate::model::VariableSizeModel;
use crate::types::{DbTransaction, Prefix};
use crate::util::ByteCast;
use crate::{define_key, variable_size_model};

variable_size_model! {
    /// Subscriber that receives events.
    #[derive(Debug, Eq, PartialEq)]
    pub struct Subscriber {
        tag: Uuid,
        _flags: u32,
        _reserved: u32,
        [pub destination_url: 0]: str,
        [pub stream_regex: 1]: str,
    }
}

define_key!(SubscriberKey {
    prefix = Prefix::Subscriber,
    id: Uuid,
});

define_key!(SubscriberTagIndexKey {
    prefix = Prefix::SubscriberTagIndex,
    tag: Uuid,
    id: Uuid,
});

define_key!(SubscriberTagPrefix {
    prefix = Prefix::SubscriberTagIndex,
    tag: Uuid,
});

impl Subscriber {
    pub fn tag(&self) -> Uuid {
        self.0.header.tag
    }

    pub fn create(txn: &DbTransaction, subscriber: &Subscriber) -> DynResult<Uuid> {
        let id = Uuid::new_v4();
        let key = SubscriberKey::new(id);
        txn.put(key, subscriber)?;
        let key = SubscriberTagIndexKey::new(subscriber.tag(), id);
        txn.put(key, &[])?;
        Ok(id)
    }

    pub fn get_by_id(txn: &DbTransaction, id: Uuid) -> Result<Option<Box<Self>>, rocksdb::Error> {
        let key = SubscriberKey::new(id);
        let bytes = txn.get(key)?;
        if let Some(bytes) = bytes {
            unsafe { Ok(Some(ByteCast::from_bytes_owned(bytes))) }
        } else {
            Ok(None)
        }
    }

    /// Iterates over subscribers by tag.
    pub fn iter_subscribers<'a>(
        txn: &'a DbTransaction,
        tag: Uuid,
    ) -> impl Iterator<Item = Result<(Uuid, Box<Self>), rocksdb::Error>> + 'a {
        let prefix = SubscriberTagPrefix::new(tag);
        txn.prefix_iterator(prefix).map(|item| {
            let (bytes, _) = item?;
            let key: SubscriberTagIndexKey =
                unsafe { *(bytes.as_ptr() as *const SubscriberTagIndexKey) };
            let id = key.id;

            let subscriber = Self::get_by_id(txn, id)?.unwrap();
            Ok((id, subscriber))
        })
    }

    /// Iterates over all subscribers of the given stream. This may be slow if
    /// there are a lot of subscribers for a given tag.
    pub fn iter_stream_subscribers<'a>(
        txn: &'a DbTransaction,
        tag: Uuid,
        stream: &'a str,
    ) -> impl Iterator<Item = Result<(Uuid, Box<Self>), rocksdb::Error>> + 'a {
        Self::iter_subscribers(txn, tag).filter_map(|item| {
            let (id, subscriber) = match item {
                Ok(x) => x,
                Err(e) => return Some(Err(e)),
            };
            let regex = Regex::new(subscriber.stream_regex()).unwrap();
            if regex.is_match(stream) {
                Some(Ok((id, subscriber)))
            } else {
                None
            }
        })
    }

    pub fn create_deliveries_for_event(
        txn: &DbTransaction,
        event_id: u64,
        event: &Event,
    ) -> DynResult<()> {
        let subscribers = Subscriber::iter_stream_subscribers(txn, event.tag(), event.stream());
        for item in subscribers {
            let (id, _) = item?;
            Delivery::create(txn, id, event_id)?;
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct SubscriberBuilder {
    tag: Uuid,
    destination_url: Option<Url>,
    stream_regex: Option<Regex>,
}

impl SubscriberBuilder {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn tag(&mut self, tag: Uuid) -> &mut Self {
        self.tag = tag;
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
        let destination_url = self
            .destination_url
            .take()
            .ok_or("Missing subscriber URL")?;
        if destination_url.scheme() != "http" && destination_url.scheme() != "https" {
            return Err("Invalid subscriber URL".into());
        }

        let stream_regex = self.stream_regex.take().ok_or("Missing subscriber regex")?;

        let subscriber = VariableSizeModel::new(
            SubscriberHeader {
                tag: self.tag,
                _reserved: 0,
                _flags: 0,
            },
            &[
                destination_url.as_ref().as_bytes(),
                stream_regex.as_str().as_bytes(),
            ],
        );

        Ok(subscriber.into())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use regex::Regex;
    use url::Url;

    use crate::subscriber::Subscriber;
    use crate::testing::TestHarness;

    use super::SubscriberBuilder;

    #[test]
    fn test_create_subscriber() {
        let url = "https://example.com/webhook";
        let regex = "^hello";
        let tag = uuid::uuid!("00000000-0000-8000-8000-000000000000");
        let subscriber = SubscriberBuilder::new()
            .tag(tag)
            .destination_url(Url::parse(url).unwrap())
            .stream_regex(Regex::new(regex).unwrap())
            .build()
            .unwrap();
        assert_eq!(subscriber.tag(), tag);
        assert_eq!(subscriber.stream_regex(), regex);
        assert_eq!(subscriber.destination_url(), url);

        let mut harness = TestHarness::new();
        let db = Arc::new(harness.db());
        let txn = db.transaction();
        let id = Subscriber::create(&txn, &subscriber).unwrap();
        txn.commit().unwrap();

        let txn = db.transaction();
        assert_eq!(
            &Subscriber::get_by_id(&txn, id).unwrap().unwrap(),
            &subscriber
        );
    }
}
