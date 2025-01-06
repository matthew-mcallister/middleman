use std::sync::Arc;

use subscriber::Subscriber;
use types::Db;

use crate::config::Config;
use crate::error::DynResult;
use crate::event::{Event, EventTable};

pub mod accessor;
pub mod key;
pub mod config;
pub mod delivery;
pub mod error;
pub mod event;
mod model;
pub mod subscriber;
#[cfg(test)]
mod testing;
pub mod types;
mod util;

pub struct Application {
    pub(crate) config: Box<Config>,
    pub(crate) db: Arc<Db>,
    pub(crate) events: Arc<EventTable>,
}

impl Application {
    pub fn new(config: Box<Config>) -> DynResult<Self> {
        let mut options = rocksdb::Options::default();
        options.create_if_missing(true);
        let db = Db::open(&options, &config.db_dir)?;
        let db = Arc::new(db);

        let events = Arc::new(EventTable::new(Arc::clone(&db)));

        Ok(Self { config, db, events })
    }

    pub fn create_event(&self, event: &Event) -> DynResult<u64> {
        let txn = self.db.transaction();

        // Construct idempotency key and check for existing record
        if let Some(id) = self
            .events
            .get_id_by_idempotency_key(event.tag(), event.idempotency_key())?
        {
            return Ok(id);
        }

        let id = self.events.create(&txn, event)?;
        Subscriber::create_deliveries_for_event(&txn, id, event)?;

        txn.commit()?;

        Ok(id)
    }
}

#[cfg(test)]
mod tests {
    use regex::Regex;
    use url::Url;

    use crate::delivery::Delivery;
    use crate::event::{ContentType, EventBuilder};
    use crate::subscriber::{Subscriber, SubscriberBuilder};
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

        let event2 = app.events.get_by_id(id).unwrap().unwrap();
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
        let subscriber1 = SubscriberBuilder::new()
            .tag(tag)
            .destination_url(Url::parse(url).unwrap())
            .stream_regex(Regex::new("^asdf:").unwrap())
            .build()
            .unwrap();
        let subscriber1_id = Subscriber::create(&txn, &subscriber1).unwrap();
        let subscriber2 = SubscriberBuilder::new()
            .tag(tag)
            .destination_url(Url::parse(url).unwrap())
            .stream_regex(Regex::new("^asdf:1234$").unwrap())
            .build()
            .unwrap();
        let subscriber2_id = Subscriber::create(&txn, &subscriber2).unwrap();
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

        let delivery1 = Delivery::get(&app.db, subscriber1_id, event_id)
            .unwrap()
            .unwrap();
        assert_eq!(delivery1.attempts_made(), 0);

        let delivery2 = Delivery::get(&app.db, subscriber2_id, event_id)
            .unwrap()
            .unwrap();
        assert_eq!(delivery2.attempts_made(), 0);
    }
}
