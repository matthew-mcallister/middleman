use std::sync::Arc;

use config::Config;
use delivery::DeliveryTable;
use error::DynResult;
use event::{Event, EventTable};
use subscriber::SubscriberTable;
use types::{Db, DbTransaction};

pub mod accessor;
pub mod big_tuple;
pub mod bytes;
pub mod comparator;
pub mod config;
pub mod delivery;
pub mod error;
pub mod event;
pub mod key;
pub mod model;
pub mod prefix;
pub mod subscriber;
#[cfg(test)]
mod testing;
pub mod types;
mod util;

pub struct Application {
    pub(crate) config: Box<Config>,
    pub(crate) events: EventTable,
    pub(crate) deliveries: DeliveryTable,
    pub(crate) subscribers: SubscriberTable,
    // XXX: Drop last
    pub(crate) db: Arc<Db>,
}

impl Application {
    pub fn new(config: Box<Config>) -> DynResult<Self> {
        let mut options = rocksdb::Options::default();
        options.create_if_missing(true);
        let db = Db::open(&options, &config.db_dir)?;
        let db = Arc::new(db);

        let events = EventTable::new(Arc::clone(&db))?;
        let deliveries = DeliveryTable::new(Arc::clone(&db))?;
        let subscribers = SubscriberTable::new(Arc::clone(&db))?;

        Ok(Self {
            config,
            db,
            events,
            deliveries,
            subscribers,
        })
    }

    pub fn create_event(&self, event: &Event) -> DynResult<u64> {
        let mut opts = rocksdb::OptimisticTransactionOptions::new();
        opts.set_snapshot(true);
        let txn = self.db.transaction_opt(&Default::default(), &opts);
        let id = self.events.create(&txn, event)?;
        self.create_deliveries_for_event(&txn, id, event)?;
        txn.commit()?;
        Ok(id)
    }

    fn create_deliveries_for_event(
        &self,
        txn: &DbTransaction,
        event_id: u64,
        event: &Event,
    ) -> DynResult<()> {
        let subscribers = self
            .subscribers
            .iter_by_stream(txn, event.tag(), event.stream());
        for subscriber in subscribers {
            self.deliveries.create(txn, subscriber?.id(), event_id)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use regex::Regex;
    use url::Url;

    use crate::event::{ContentType, EventBuilder};
    use crate::subscriber::SubscriberBuilder;
    use crate::testing::TestHarness;

    #[test]
    fn test_create_event_idempotency() {
        let mut harness = TestHarness::new();
        let app = harness.application();

        let tag = uuid::uuid!("00000000-0000-8000-8000-000000000000");
        let idempotency_key = uuid::uuid!("00000000-0000-8000-8000-000000000001");
        let event = EventBuilder::new()
            .content_type(ContentType::Json)
            .tag(tag)
            .stream("asdf")
            .payload(b"1234321")
            .idempotency_key(idempotency_key)
            .build();
        let id = app.create_event(&event).unwrap();

        let event2 = app.events.get(id).unwrap().unwrap();
        assert_eq!(*event, *event2);

        let id2 = app.create_event(&event).unwrap();
        assert_eq!(id, id2);
    }

    #[test]
    fn test_create_event_with_subscribers() {
        let mut harness = TestHarness::new();
        let app = harness.application();

        let tag = uuid::uuid!("00000000-0000-8000-8000-000000000000");

        // Create two subscribers
        let txn = app.db.transaction();
        let url = "https://example.com/webhook";
        let subscriber1_id = uuid::uuid!("12120000-0000-8000-8000-000000000001");
        let subscriber1 = SubscriberBuilder::new()
            .tag(tag)
            .destination_url(Url::parse(url).unwrap())
            .stream_regex(Regex::new("^asdf:").unwrap())
            .build()
            .unwrap();
        app.subscribers.create(&txn, &subscriber1).unwrap();
        let subscriber2_id = uuid::uuid!("12120000-0000-8000-8000-000000000002");
        let subscriber2 = SubscriberBuilder::new()
            .tag(tag)
            .destination_url(Url::parse(url).unwrap())
            .stream_regex(Regex::new("^asdf:1234$").unwrap())
            .build()
            .unwrap();
        app.subscribers.create(&txn, &subscriber2).unwrap();
        txn.commit().unwrap();

        // Create an event that matches both subscribers
        let idempotency_key = uuid::uuid!("00000000-0000-8000-8000-000000000000");
        let event = EventBuilder::new()
            .content_type(ContentType::Json)
            .tag(tag)
            .stream("asdf:1234")
            .payload(b"1234321")
            .idempotency_key(idempotency_key)
            .build();
        let event_id = app.create_event(&event).unwrap();

        let delivery1 = app
            .deliveries
            .get(subscriber1_id, event_id)
            .unwrap()
            .unwrap();
        assert_eq!(delivery1.attempts_made(), 0);

        let delivery2 = app
            .deliveries
            .get(subscriber2_id, event_id)
            .unwrap()
            .unwrap();
        assert_eq!(delivery2.attempts_made(), 0);
    }
}
