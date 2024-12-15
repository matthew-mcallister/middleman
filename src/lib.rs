use std::sync::Arc;

use subscriber::Subscriber;

use crate::config::Config;
use crate::error::DynResult;
use crate::event::{Event, EventTable};

pub mod config;
pub mod delivery;
pub mod error;
pub mod event;
pub mod subscriber;
#[cfg(test)]
mod testing;
pub mod types;
mod util;

pub struct Middleman {
    pub(crate) db: Arc<rocksdb::OptimisticTransactionDB>,
    pub(crate) events: Arc<EventTable>,
}

impl Middleman {
    pub fn new(config: Config) -> DynResult<Self> {
        let mut options = rocksdb::Options::default();
        options.create_if_missing(true);
        let db = rocksdb::OptimisticTransactionDB::open(&options, &config.db_dir)?;
        let db = Arc::new(db);

        let events = Arc::new(EventTable::new(Arc::clone(&db)));

        Ok(Self { db, events })
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

        Ok(id)
    }
}
