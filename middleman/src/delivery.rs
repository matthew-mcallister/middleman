use chrono::{Datelike, Timelike, Utc};
use uuid::Uuid;

use crate::bytes::ByteCast;
use crate::error::DynResult;
use crate::types::{Db, DbTransaction, Prefix};
use crate::{define_key, impl_byte_cast};

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

impl From<DateTime> for chrono::DateTime<chrono::Utc> {
    fn from(value: DateTime) -> Self {
        let naive: chrono::NaiveDateTime = value.into();
        chrono::DateTime::from_naive_utc_and_offset(naive, chrono::Utc)
    }
}

impl From<chrono::DateTime<chrono::Utc>> for DateTime {
    fn from(value: chrono::DateTime<chrono::Utc>) -> Self {
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
#[derive(Debug, PartialEq, Eq)]
#[repr(C)]
pub struct Delivery {
    subscriber_id: Uuid,
    event_id: u64,
    next_attempt: DateTime,
    attempts_made: u32,
}

impl_byte_cast!(Delivery);

define_key!(DeliveryKey {
    prefix = Prefix::Delivery,
    subscriber_id: Uuid,
    event_id: u64,
});

impl AsRef<[u8]> for Delivery {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl Delivery {
    pub(crate) fn create(
        txn: &DbTransaction,
        subscriber_id: Uuid,
        event_id: u64,
    ) -> DynResult<Self> {
        let delivery = Delivery {
            subscriber_id,
            event_id,
            attempts_made: 0,
            next_attempt: Utc::now().into(),
        };

        let key = DeliveryKey::new(subscriber_id, event_id);
        txn.put(key, &delivery)?;

        Ok(delivery)
    }

    pub(crate) fn get(db: &Db, subscriber_id: Uuid, event_id: u64) -> DynResult<Option<Box<Self>>> {
        let key = DeliveryKey::new(subscriber_id, event_id);
        let bytes = match db.get(key)? {
            Some(bytes) => bytes,
            None => return Ok(None),
        };
        unsafe { Ok(Some(ByteCast::from_bytes_owned(bytes))) }
    }

    pub(crate) fn as_bytes(&self) -> &[u8] {
        unsafe { std::mem::transmute(ByteCast::as_bytes(self)) }
    }

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
    use crate::testing::TestHarness;

    use super::Delivery;

    #[test]
    fn test_create_delivery() {
        let mut harness = TestHarness::new();
        let db = harness.db();

        let subscriber_id = uuid::uuid!("00000000-0000-8000-8000-000000000000");
        let event_id: u64 = 1;
        let txn = db.transaction();
        let delivery = Delivery::create(&txn, subscriber_id, event_id).unwrap();
        txn.commit().unwrap();

        let delivery2 = Delivery::get(&db, subscriber_id, event_id)
            .unwrap()
            .unwrap();
        assert_eq!(delivery, *delivery2);
    }
}
