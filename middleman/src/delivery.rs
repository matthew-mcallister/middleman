use std::sync::Arc;

use bytecast::{FromBytes, HasLayout, IntoBytes, Unalign};
use chrono::{Datelike, Timelike, Utc};
use middleman_db::key::{BigEndianU32, BigEndianU64, Packed2, Packed3};
use middleman_db::prefix::IsPrefixOf;
use middleman_db::{
    packed, Accessor, ColumnFamily, ColumnFamilyDescriptor, Cursor, Db, Transaction,
};
use rand::Rng;
use tracing::debug;
use uuid::Uuid;

use crate::db::ColumnFamilyName;
use crate::error::Result;

const MAX_ATTEMPTS: u32 = 19;

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
#[derive(Clone, Copy, Debug, Eq, FromBytes, HasLayout, IntoBytes, Ord, PartialEq, PartialOrd)]
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
#[derive(Clone, Copy, Debug, FromBytes, HasLayout, IntoBytes, PartialEq, Eq)]
#[repr(C)]
pub struct Delivery {
    tag: [u8; 16],
    subscriber_id: [u8; 16],
    event_id: u64,
    _reserved: [u64; 2],
    next_attempt: DateTime,
    attempts_made: u32,
}

impl Delivery {
    pub(crate) fn tag(&self) -> Uuid {
        Uuid::from_bytes(self.tag)
    }

    pub(crate) fn subscriber_id(&self) -> Uuid {
        Uuid::from_bytes(self.subscriber_id)
    }

    pub(crate) fn event_id(&self) -> u64 {
        self.event_id
    }

    pub(crate) fn next_attempt(&self) -> chrono::DateTime<Utc> {
        self.next_attempt.into()
    }

    fn next_attempt_index_key(&self) -> NextAttemptKey {
        NextAttemptKey {
            tag: self.tag,
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

#[derive(Debug)]
pub struct DeliveryTable {
    pub(crate) cf: ColumnFamily,
    next_attempt_index: ColumnFamily,
}

type DeliveryKey = Packed3<BigEndianU64, [u8; 16], [u8; 16]>;

// TODO: Add a global next attempt index with the subscriber as suffix rather
// than prefix. Then support both "fast scan" over all subscribers and "fair
// scan" done per-subscriber.
#[derive(Clone, Copy, Debug, Eq, FromBytes, HasLayout, IntoBytes, Ord, PartialEq, PartialOrd)]
#[repr(C)]
pub(crate) struct NextAttemptKey {
    tag: [u8; 16],
    subscriber_id: [u8; 16],
    yof: BigEndianU32,
    seconds: BigEndianU32,
    nanos: BigEndianU32,
    event_id: BigEndianU64,
}

impl NextAttemptKey {
    fn subscriber_id(&self) -> Uuid {
        Uuid::from_bytes(self.subscriber_id)
    }

    fn timestamp(&self) -> DateTime {
        DateTime {
            yof: self.yof.into(),
            seconds: self.seconds.into(),
            nanos: self.nanos.into(),
        }
    }
}

impl IsPrefixOf<NextAttemptKey> for [u8; 16] {
    fn is_prefix_of(&self, other: &NextAttemptKey) -> bool {
        *self == other.subscriber_id
    }
}

#[derive(Debug)]
pub(crate) struct PendingDelivery {
    pub(crate) transaction: Transaction,
    pub(crate) tag: Uuid,
    pub(crate) subscriber_id: Uuid,
    pub(crate) event_id: u64,
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

    fn accessor<'a>(&'a self) -> Accessor<'a, DeliveryKey, Unalign<Delivery>> {
        Accessor::new(&self.cf)
    }

    fn next_attempt_index_accessor<'a>(&'a self) -> Accessor<'a, NextAttemptKey, ()> {
        Accessor::new(&self.next_attempt_index)
    }

    pub(crate) fn db(&self) -> &Arc<Db> {
        &self.cf.db()
    }

    pub(crate) fn create(
        &self,
        txn: &mut Transaction,
        // n.b. this tag is technically redundant
        tag: Uuid,
        subscriber_id: Uuid,
        event_id: u64,
    ) -> Delivery {
        let next_attempt = Utc::now().into();
        let delivery = Delivery {
            tag: tag.into_bytes(),
            subscriber_id: subscriber_id.into_bytes(),
            event_id,
            attempts_made: 0,
            next_attempt,
            _reserved: [0; 2],
        };
        self.accessor().put_txn(
            txn,
            &packed!(event_id.into(), tag.into_bytes(), subscriber_id.into_bytes()),
            &Unalign(delivery),
        );
        let index_key = delivery.next_attempt_index_key();
        self.next_attempt_index_accessor().put_txn(txn, &index_key, &());
        delivery
    }

    pub(crate) fn get(
        &self,
        tag: Uuid,
        subscriber_id: Uuid,
        event_id: u64,
    ) -> Result<Option<Delivery>> {
        Ok(self
            .accessor()
            .get(&packed!(event_id.into(), tag.into_bytes(), subscriber_id.into_bytes()))?
            .map(|x| x.into_inner()))
    }

    pub(crate) fn iter<'a>(&'a self) -> impl Iterator<Item = Result<Delivery>> + 'a {
        let mut cursor = self.accessor().cursor();
        cursor.seek_to_first();
        cursor.values().map(|r| r.map(Unalign::into_inner).map_err(Into::into))
    }

    pub(crate) fn iter_by_next_attempt_skip_locked<'a>(
        &'a self,
        tag: Uuid,
        subscriber_id: Uuid,
        max_time: chrono::DateTime<chrono::Utc>,
    ) -> impl Iterator<Item = Result<PendingDelivery>> + 'a {
        let max_time: DateTime = max_time.into();
        let mut done = false;
        self.next_attempt_index_accessor()
            .cursor()
            .prefix_iter::<Packed2<[u8; 16], [u8; 16]>>(packed!(
                tag.into_bytes(),
                subscriber_id.into_bytes()
            ))
            .keys()
            .filter_map(move |key| {
                if done {
                    return None;
                }

                let key = match key {
                    Ok(key) => key,
                    Err(e) => return Some(Err(e.into())),
                };

                if key.timestamp() >= max_time {
                    done = true;
                    return None;
                }

                let NextAttemptKey {
                    tag,
                    subscriber_id,
                    event_id,
                    ..
                } = key;
                let key: DeliveryKey = packed!(event_id, tag, subscriber_id);
                let transaction =
                    self.db().begin_transaction_locked(&self.cf, IntoBytes::as_bytes(&key));

                match transaction {
                    Ok(transaction) => Some(Ok(PendingDelivery {
                        transaction,
                        tag: Uuid::from_bytes(tag),
                        subscriber_id: Uuid::from_bytes(subscriber_id),
                        event_id: event_id.into(),
                    })),
                    Err(_) => None,
                }
            })
    }

    pub(crate) fn delete(&self, txn: &mut Transaction, delivery: &Delivery) {
        let key = packed!(delivery.event_id.into(), delivery.tag, delivery.subscriber_id);
        self.accessor().delete_txn(txn, &key);
        let key = delivery.next_attempt_index_key();
        self.next_attempt_index_accessor().delete_txn(txn, &key);
    }

    pub(crate) fn update_for_next_attempt(&self, txn: &mut Transaction, delivery: &mut Delivery) {
        let attempts_made = delivery.attempts_made;
        delivery.attempts_made += 1;
        if delivery.attempts_made == MAX_ATTEMPTS {
            debug!(
                "delivery of event {} to subscriber {} failed after {} attempts",
                delivery.event_id(),
                delivery.subscriber_id(),
                MAX_ATTEMPTS,
            );
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
        let key = packed!(delivery.event_id.into(), delivery.tag, delivery.subscriber_id);
        self.accessor().put_txn(txn, &key, Unalign::from_ref(delivery));
        let key = delivery.next_attempt_index_key();
        self.next_attempt_index_accessor().put_txn(txn, &key, &());
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

        let tag = uuid::uuid!("10000000-0000-8000-8000-000000000000");
        let subscriber_id = uuid::uuid!("00000000-0000-8000-8000-000000000000");
        let event_id: u64 = 1;
        let mut txn = Transaction::new(Arc::clone(&app.db));
        let delivery = deliveries.create(&mut txn, tag, subscriber_id, event_id);
        txn.commit().unwrap();

        let delivery2 = deliveries.get(tag, subscriber_id, event_id).unwrap().unwrap();
        assert_eq!(delivery, delivery2);
    }
}
