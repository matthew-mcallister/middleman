use std::sync::Arc;

use chrono::{Datelike, Timelike, Utc};
use uuid::Uuid;

use crate::accessor::CfAccessor;
use crate::bytes::AsBytes;
use crate::error::DynResult;
use crate::key::Packed2;
use crate::types::{Db, DbColumnFamily, DbTransaction};
use crate::util::get_or_create_cf;

// UTC datetime with stable binary representation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(C)]
pub struct DateTime {
    yof: i32,
    seconds: u32,
    nanos: u32,
}

impl From<DateTime> for chrono::NaiveDateTime {
    fn from(value: DateTime) -> Self {
        let year = value.yof >> 9;
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
        let yof = year << 9 | (day as i32);
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

pub struct DeliveryTable {
    cf: DbColumnFamily,
    // XXX: Drop last
    db: Arc<Db>,
}

impl DeliveryTable {
    pub(crate) fn new(db: Arc<Db>) -> DynResult<Self> {
        let cf = unsafe { get_or_create_cf(&db, "deliveries", &Default::default())? };
        Ok(Self { db, cf })
    }

    fn accessor<'a>(&'a self) -> CfAccessor<'a, Packed2<Uuid, u64>, Delivery> {
        CfAccessor::new(&self.db, &self.cf)
    }

    pub(crate) fn create(
        &self,
        txn: &DbTransaction,
        subscriber_id: Uuid,
        event_id: u64,
    ) -> DynResult<Delivery> {
        let delivery = Delivery {
            subscriber_id,
            event_id,
            attempts_made: 0,
            next_attempt: Utc::now().into(),
            _reserved: [0; 2],
        };
        self.accessor()
            .put_txn(&txn, &(subscriber_id, event_id).into(), &delivery)?;
        Ok(delivery)
    }

    pub(crate) fn get(&self, subscriber_id: Uuid, event_id: u64) -> DynResult<Option<Delivery>> {
        unsafe {
            Ok(self
                .accessor()
                .get_unchecked(&(subscriber_id, event_id).into())?)
        }
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

    pub(crate) fn attempts_made(&self) -> u32 {
        self.attempts_made
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{delivery::DeliveryTable, testing::TestHarness};

    #[test]
    fn test_create_delivery() {
        let mut harness = TestHarness::new();
        let db = harness.db();
        let table = DeliveryTable::new(Arc::clone(&db)).unwrap();

        let subscriber_id = uuid::uuid!("00000000-0000-8000-8000-000000000000");
        let event_id: u64 = 1;
        let txn = db.transaction();
        let delivery = table.create(&txn, subscriber_id, event_id).unwrap();
        txn.commit().unwrap();

        let delivery2 = table.get(subscriber_id, event_id).unwrap().unwrap();
        assert_eq!(delivery, delivery2);
    }
}
