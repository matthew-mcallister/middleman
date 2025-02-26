use std::sync::Arc;

use db::{Db, Transaction};
use middleman_db as db;

use crate::config::Config;
use crate::delivery::DeliveryTable;
use crate::error::Result;
use crate::event::{Event, EventTable};
use crate::migration::Migrator;
use crate::subscriber::SubscriberTable;

pub struct Application {
    pub(crate) config: Box<Config>,
    pub(crate) db: Arc<Db>,
    pub(crate) events: EventTable,
    pub(crate) deliveries: DeliveryTable,
    pub(crate) subscribers: SubscriberTable,
}

impl Application {
    pub fn new(config: Box<Config>) -> Result<Self> {
        let mut migrator = Migrator::new(&config.db_dir)?;
        migrator.migrate()?;
        let db = Arc::new(migrator.unwrap());

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

    pub fn create_event(&self, event: &Event) -> Result<u64> {
        let mut opts = rocksdb::OptimisticTransactionOptions::new();
        opts.set_snapshot(true);
        let mut txn = Transaction::new(&self.db);
        let id = self.events.create(&mut txn, event)?;
        self.create_deliveries_for_event(&mut txn, id, event)?;
        txn.commit()?;
        Ok(id)
    }

    fn create_deliveries_for_event(
        &self,
        txn: &mut Transaction,
        event_id: u64,
        event: &Event,
    ) -> Result<()> {
        let subscribers = self.subscribers.iter_by_stream(event.tag(), event.stream());
        for subscriber in subscribers {
            self.deliveries.create(txn, subscriber?.id(), event_id);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use byteview::ByteView;
    use db::Transaction;
    use middleman_db as db;
    use regex::Regex;
    use url::Url;

    use crate::event::EventBuilder;
    use crate::subscriber::SubscriberBuilder;
    use crate::testing::TestHarness;
    use crate::types::ContentType;

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

        let event2 = app.events.get(tag, id).unwrap().unwrap();
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
        let mut txn = Transaction::new(&app.db);
        let url = "https://example.com/webhook";
        let subscriber1_id = uuid::uuid!("12120000-0000-8000-8000-000000000001");
        let subscriber1 = SubscriberBuilder::new()
            .tag(tag)
            .id(subscriber1_id)
            .destination_url(Url::parse(url).unwrap())
            .stream_regex(Regex::new("^asdf:").unwrap())
            .build()
            .unwrap();
        app.subscribers.create(&mut txn, &subscriber1).unwrap();
        let subscriber2_id = uuid::uuid!("12120000-0000-8000-8000-000000000002");
        let subscriber2 = SubscriberBuilder::new()
            .tag(tag)
            .id(subscriber2_id)
            .destination_url(Url::parse(url).unwrap())
            .stream_regex(Regex::new("^asdf:1234$").unwrap())
            .build()
            .unwrap();
        app.subscribers.create(&mut txn, &subscriber2).unwrap();
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

        let delivery1 = app.deliveries.get(subscriber1_id, event_id).unwrap().unwrap();
        assert_eq!(delivery1.attempts_made(), 0);

        let delivery2 = app.deliveries.get(subscriber2_id, event_id).unwrap().unwrap();
        assert_eq!(delivery2.attempts_made(), 0);
    }

    // Creating a transaction when one already exists for the given key fails
    #[test]
    fn test_transaction_lock() {
        let mut harness = TestHarness::new();
        let app = harness.application();

        let key: ByteView = [1u8, 2, 3, 4].into();
        let mut txn1 = Transaction::new(&app.db);
        txn1.lock_key(key.clone()).unwrap();
        let mut txn2 = Transaction::new(&app.db);
        assert_eq!(
            txn2.lock_key(key).unwrap_err().kind(),
            db::ErrorKind::TransactionConflict
        );
    }
}
