use std::sync::Arc;

use db::{Db, Transaction};
use middleman_db as db;
use tracing::debug;
use uuid::Uuid;

use crate::config::Config;
use crate::connection::Http11ConnectionPoolSettings;
use crate::delivery::DeliveryTable;
use crate::error::Result;
use crate::event::{Event, EventBuilder, EventTable};
use crate::http::{SubscriberConnectionFactory, SubscriberConnectionPool};
use crate::migration::Migrator;
use crate::scheduler::Scheduler;
use crate::subscriber::{Subscriber, SubscriberBuilder, SubscriberTable};

#[derive(Debug)]
pub struct Application {
    pub(crate) config: Box<Config>,
    pub(crate) db: Arc<Db>,
    pub(crate) events: Arc<EventTable>,
    pub(crate) deliveries: Arc<DeliveryTable>,
    pub(crate) subscribers: Arc<SubscriberTable>,
    pub(crate) connections: Arc<SubscriberConnectionPool>,
    pub(crate) scheduler: Scheduler,
}

impl Application {
    pub fn new(config: Box<Config>) -> Result<Self> {
        let mut migrator = Migrator::new(&config.db_dir)?;
        migrator.migrate()?;
        let db = Arc::new(migrator.unwrap());

        let events = Arc::new(EventTable::new(Arc::clone(&db))?);
        let deliveries = Arc::new(DeliveryTable::new(Arc::clone(&db))?);
        let subscribers = Arc::new(SubscriberTable::new(Arc::clone(&db))?);

        let settings = Http11ConnectionPoolSettings::default();
        let connection_factory =
            Box::new(SubscriberConnectionFactory::new(Arc::clone(&subscribers))?);
        let connections = Arc::new(SubscriberConnectionPool::new(settings, connection_factory));

        let scheduler = Scheduler::new(
            Arc::clone(&subscribers),
            Arc::clone(&events),
            Arc::clone(&deliveries),
            Arc::clone(&connections),
        )?;

        Ok(Self {
            config,
            db,
            events,
            deliveries,
            subscribers,
            connections,
            scheduler,
        })
    }

    pub fn create_event(&self, builder: EventBuilder<'_>) -> Result<Box<Event>> {
        let mut opts = rocksdb::OptimisticTransactionOptions::new();
        opts.set_snapshot(true);
        let mut txn = Transaction::new(Arc::clone(&self.db));
        let event = self.events.create(&mut txn, builder)?;
        self.create_deliveries_for_event(&mut txn, &event)?;
        txn.commit()?;
        debug!(?event, "Created event {}", event.id());
        // TODO: Schedule event to be delivered if queue is not too full
        Ok(event)
    }

    fn create_deliveries_for_event(&self, txn: &mut Transaction, event: &Event) -> Result<()> {
        let subscribers = self.subscribers.iter_by_stream(event.tag(), event.stream());
        for subscriber in subscribers {
            self.deliveries.create(txn, event.tag(), subscriber?.id(), event.id());
        }
        Ok(())
    }

    pub fn create_subscriber(&self, builder: SubscriberBuilder) -> Result<Box<Subscriber>> {
        let mut txn = self.db.begin_transaction();
        let subscriber = self.subscribers.create(&mut txn, builder)?;
        txn.commit()?;
        self.scheduler.register_subscriber(&subscriber);
        Ok(subscriber)
    }

    pub fn delete_subscriber(&self, tag: Uuid, id: Uuid) -> Result<()> {
        let mut txn = self.db.begin_transaction();
        self.subscribers.delete(&mut txn, tag, id)?;
        txn.commit()?;
        Ok(())
    }

    pub fn schedule_deliveries(&self) -> Result<()> {
        self.scheduler.schedule_all()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use byteview::ByteView;
    use db::Transaction;
    use middleman_db::{self as db, DbOptions};
    use regex::Regex;
    use url::Url;

    use crate::event::EventBuilder;
    use crate::subscriber::SubscriberBuilder;
    use crate::testing::TestHarness;

    #[test]
    fn test_create_event_idempotency() {
        let mut harness = TestHarness::new();
        let app = harness.application();

        let tag = uuid::uuid!("00000000-0000-8000-8000-000000000000");
        let idempotency_key = uuid::uuid!("00000000-0000-8000-8000-000000000001");
        let mut builder = EventBuilder::new();
        builder
            .tag(tag)
            .stream("asdf")
            .payload("1234321")
            .idempotency_key(idempotency_key);
        let event = app.create_event(builder.clone()).unwrap();

        let event2 = app.events.get(event.tag(), event.id()).unwrap().unwrap();
        assert_eq!(event, event2);

        let event2 = app.create_event(builder).unwrap();
        assert_eq!(event, event2);
    }

    #[test]
    fn test_create_event_with_subscribers() {
        let mut harness = TestHarness::new();
        let app = harness.application();

        let tag = uuid::uuid!("00000000-0000-8000-8000-000000000000");

        // Create two subscribers
        let url = "https://example.com/webhook";
        let mut builder = SubscriberBuilder::new();
        builder
            .id(uuid::uuid!("00000000-0000-8000-8000-000000000001"))
            .tag(tag)
            .destination_url(Url::parse(url).unwrap())
            .stream_regex(Regex::new("^asdf:").unwrap())
            .hmac_key("key".to_owned());
        let subscriber1 = app.create_subscriber(builder).unwrap();
        let mut builder = SubscriberBuilder::new();
        builder
            .id(uuid::uuid!("00000000-0000-8000-8000-000000000002"))
            .tag(tag)
            .destination_url(Url::parse(url).unwrap())
            .stream_regex(Regex::new("^asdf:1234$").unwrap())
            .hmac_key("key".to_owned());
        let subscriber2 = app.create_subscriber(builder).unwrap();

        // Create an event that matches both subscribers
        let idempotency_key = uuid::uuid!("00000000-0000-8000-8000-000000000000");
        let mut event = EventBuilder::new();
        event
            .tag(tag)
            .stream("asdf:1234")
            .payload("1234321")
            .idempotency_key(idempotency_key);
        let event = app.create_event(event).unwrap();

        let delivery1 = app
            .deliveries
            .get(subscriber1.tag(), subscriber1.id(), event.id())
            .unwrap()
            .unwrap();
        assert_eq!(delivery1.attempts_made(), 0);

        let delivery2 = app
            .deliveries
            .get(subscriber2.tag(), subscriber2.id(), event.id())
            .unwrap()
            .unwrap();
        assert_eq!(delivery2.attempts_made(), 0);
    }

    // Creating a transaction when one already exists for the given key fails
    #[test]
    fn test_transaction_lock() {
        let mut harness = TestHarness::new();

        let descs: [(&str, &rocksdb::Options, rocksdb::ColumnFamilyTtl); 0] = [];
        let mut options = DbOptions::default();
        options.create_if_missing = true;
        let mut db = middleman_db::Db::open(harness.db_dir(), &options, descs).unwrap();
        db.create_column_family(&("cf", &Default::default(), Default::default()))
            .unwrap();
        let db = Arc::new(db);
        let cf = db.get_column_family("cf").unwrap();

        let key: ByteView = [1u8, 2, 3, 4].into();
        let mut txn1 = Transaction::new(Arc::clone(&db));
        txn1.lock_key(&cf, key.clone()).unwrap();
        let mut txn2 = Transaction::new(Arc::clone(&db));
        assert_eq!(txn2.lock_key(&cf, key).unwrap_err().kind(), db::ErrorKind::TransactionConflict);
    }
}
