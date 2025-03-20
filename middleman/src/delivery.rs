use std::sync::Arc;

use chrono::{Datelike, Timelike, Utc};
use db::bytes::AsBytes;
use db::key::Packed2;
use db::{Accessor, ColumnFamily, ColumnFamilyDescriptor, Db, Transaction};
use middleman_db::key::{BigEndianU32, BigEndianU64};
use middleman_db::{self as db, packed, Cursor};
use middleman_macros::db_key;
use rand::Rng;
use uuid::Uuid;

use crate::db::ColumnFamilyName;
use crate::error::Result;

const MAX_ATTEMPTS: u32 = 12;

/// Returns a random duration to wait before the next attempt, with
/// exponential backoff.
fn next_attempt_delay(last_attempt: u32) -> chrono::TimeDelta {
    let window = 2f32.powi(last_attempt as _);
    let x: f32 = rand::rng().random();
    let delay = window * x + 1.0;
    let (secs, nanos) = (delay.floor(), (delay.fract() * 1e9).round());
    chrono::TimeDelta::new(secs as i64, nanos as u32).unwrap()
}

// UTC datetime with stable binary representation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(C)]
pub struct DateTime {
    yof: u32,
    seconds: u32,
    nanos: u32,
}

impl From<DateTime> for chrono::NaiveDateTime {
    fn from(value: DateTime) -> Self {
        let year = i32::try_from(value.yof >> 9).unwrap();
        let day = (value.yof & 0x1ff) as u32;
        let date = chrono::NaiveDate::from_yo_opt(year, day).unwrap();
        let time =
            chrono::NaiveTime::from_num_seconds_from_midnight_opt(value.seconds, value.nanos)
                .unwrap();
        chrono::NaiveDateTime::new(date, time)
    }
}

impl From<DateTime> for chrono::DateTime<Utc> {
    fn from(value: DateTime) -> Self {
        let naive: chrono::NaiveDateTime = value.into();
        chrono::DateTime::from_naive_utc_and_offset(naive, Utc)
    }
}

impl From<chrono::DateTime<Utc>> for DateTime {
    fn from(value: chrono::DateTime<Utc>) -> Self {
        let year = value.year();
        let day = value.day();
        let yof = u32::try_from(year).unwrap() << 9 | day;
        let seconds = value.num_seconds_from_midnight();
        let nanos = value.nanosecond();
        DateTime {
            yof,
            seconds,
            nanos,
        }
    }
}

/// Delivery of a single event.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(C)]
pub struct Delivery {
    subscriber_id: Uuid,
    event_id: u64,
    _reserved: [u64; 2],
    next_attempt: DateTime,
    attempts_made: u32,
}

unsafe impl AsBytes for Delivery {}

#[derive(Debug)]
pub struct DeliveryTable {
    pub(crate) cf: ColumnFamily,
    next_attempt_index: ColumnFamily,
}

type DeliveryKey = Packed2<u64, Uuid>;

#[db_key]
pub(crate) struct NextAttemptKey {
    subscriber_id: Uuid,
    yof: BigEndianU32,
    seconds: BigEndianU32,
    nanos: BigEndianU32,
    event_id: BigEndianU64,
}

impl DeliveryTable {
    pub(crate) fn new(db: Arc<Db>) -> Result<Self> {
        let cf = db.get_column_family(ColumnFamilyName::Deliveries.name()).unwrap();
        let next_attempt_index =
            db.get_column_family(ColumnFamilyName::DeliveryNextAttemptIndex.name()).unwrap();
        Ok(Self {
            cf,
            next_attempt_index,
        })
    }

    fn accessor<'a>(&'a self) -> Accessor<'a, DeliveryKey, Delivery> {
        Accessor::new(&self.cf)
    }

    fn next_attempt_index_accessor<'a>(&'a self) -> Accessor<'a, NextAttemptKey, ()> {
        Accessor::new(&self.cf)
    }

    pub(crate) fn create(
        &self,
        txn: &mut Transaction,
        subscriber_id: Uuid,
        event_id: u64,
    ) -> Delivery {
        let next_attempt = Utc::now().into();
        let delivery = Delivery {
            subscriber_id,
            event_id,
            attempts_made: 0,
            next_attempt,
            _reserved: [0; 2],
        };
        self.accessor().put_txn(txn, &packed!(event_id, subscriber_id), &delivery);
        let index_key = delivery.next_attempt_index_key();
        self.next_attempt_index_accessor().put_txn(txn, &index_key, &());
        delivery
    }

    pub(crate) fn get(&self, subscriber_id: Uuid, event_id: u64) -> Result<Option<Delivery>> {
        unsafe { Ok(self.accessor().get_unchecked(&(event_id, subscriber_id).into())?.map(|x| *x)) }
    }

    pub(crate) fn iter<'a>(
        &'a self,
    ) -> impl Iterator<Item = db::Result<(DeliveryKey, Delivery)>> + 'a {
        unsafe { self.accessor().cursor_unchecked().into_iter() }
    }

    pub(crate) fn iter_by_next_attempt<'a>(
        &'a self,
    ) -> impl Iterator<Item = Result<Delivery>> + 'a {
        unsafe {
            self.next_attempt_index_accessor().cursor_unchecked().keys().map(|key| {
                let key = key?;
                let (subscriber_id, event_id) = (key.subscriber_id, key.event_id);
                self.get(subscriber_id, event_id.into()).transpose().unwrap()
            })
        }
    }

    pub(crate) fn delete(&self, txn: &mut Transaction, delivery: &Delivery) {
        let key = packed!(delivery.event_id, delivery.subscriber_id);
        self.accessor().delete_txn(txn, &key);
        let key = delivery.next_attempt_index_key();
        self.next_attempt_index_accessor().delete_txn(txn, &key);
    }

    pub(crate) fn update_for_next_attempt(&self, txn: &mut Transaction, delivery: &mut Delivery) {
        let attempts_made = delivery.attempts_made;
        delivery.attempts_made += 1;
        if delivery.attempts_made == MAX_ATTEMPTS {
            self.delete(txn, &delivery);
            return;
        }

        // Delete old index
        //
        // Ugh this is going to create so many tombstones lol
        // It probably makes more sense to switch to FIFO compaction and just
        // never explicitly delete old delivery objects
        let old_key = delivery.next_attempt_index_key();
        self.next_attempt_index_accessor().delete_txn(txn, &old_key);

        // Update next attempt
        let delay = next_attempt_delay(attempts_made);
        delivery.next_attempt = Utc::now().checked_add_signed(delay).unwrap().into();

        // Insert new data
        let key = packed!(delivery.event_id, delivery.subscriber_id);
        self.accessor().put_txn(txn, &key, delivery);
        let key = delivery.next_attempt_index_key();
        self.next_attempt_index_accessor().put_txn(txn, &key, &());
    }
}

impl Delivery {
    pub(crate) fn subscriber_id(&self) -> Uuid {
        self.subscriber_id
    }

    pub(crate) fn event_id(&self) -> u64 {
        self.event_id
    }

    pub(crate) fn next_attempt(&self) -> chrono::DateTime<Utc> {
        self.next_attempt.into()
    }

    fn next_attempt_index_key(&self) -> NextAttemptKey {
        NextAttemptKey {
            subscriber_id: self.subscriber_id,
            yof: self.next_attempt.yof.into(),
            seconds: self.next_attempt.seconds.into(),
            nanos: self.next_attempt.nanos.into(),
            event_id: self.event_id.into(),
        }
    }

    pub(crate) fn attempts_made(&self) -> u32 {
        self.attempts_made
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use middleman_db::Transaction;

    use crate::testing::TestHarness;

    #[test]
    fn test_create_delivery() {
        let mut harness = TestHarness::new();
        let app = harness.application();
        let deliveries = &app.deliveries;

        let subscriber_id = uuid::uuid!("00000000-0000-8000-8000-000000000000");
        let event_id: u64 = 1;
        let mut txn = Transaction::new(Arc::clone(&app.db));
        let delivery = deliveries.create(&mut txn, subscriber_id, event_id);
        txn.commit().unwrap();

        let delivery2 = deliveries.get(subscriber_id, event_id).unwrap().unwrap();
        assert_eq!(delivery, delivery2);
    }
}
