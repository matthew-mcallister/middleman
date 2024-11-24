use chrono::{NaiveDateTime, Utc};

use crate::error::DynResult;
use crate::make_key;
use crate::types::{DbTransaction, Prefix};
use crate::util::ByteCast;

/// Delivery of a single event.
#[derive(Debug, PartialEq, Eq)]
#[repr(C)]
pub struct Delivery {
    subscriber_id: u128,
    event_id: u64,
    next_attempt: NaiveDateTime,
    attempts_made: u32,
}

impl Delivery {
    pub(crate) fn create(
        txn: &DbTransaction,
        subscriber_id: u128,
        event_id: u64,
    ) -> DynResult<Self> {
        let delivery = Delivery {
            subscriber_id,
            event_id,
            attempts_made: 0,
            next_attempt: Utc::now().naive_utc(),
        };

        let key = make_key!(u8: Prefix::Delivery as u8, u128: subscriber_id, u64: event_id);
        txn.put(key, delivery.as_bytes())?;

        Ok(delivery)
    }

    pub(crate) fn as_bytes(&self) -> &[u8] {
        unsafe { ByteCast::as_bytes(self) }
    }
}

#[cfg(test)]
mod tests {
    use crate::make_key;
    use crate::testing::TestHarness;
    use crate::types::Prefix;
    use crate::util::ByteCast;

    use super::Delivery;

    #[test]
    fn test_create_delivery() {
        let mut harness = TestHarness::new();
        let db = harness.db();

        let subscriber_id: u128 = 1;
        let event_id: u64 = 2;
        let txn = db.transaction();
        let delivery = Delivery::create(&txn, subscriber_id, event_id).unwrap();
        txn.commit().unwrap();

        let key = make_key!(u8: Prefix::Delivery as u8, u128: subscriber_id, u64: event_id);
        let bytes = db.get(&key).unwrap().unwrap();
        assert_eq!(delivery.as_bytes(), bytes);

        let delivery2: Box<Delivery> = unsafe { ByteCast::from_bytes_owned(bytes) };
        assert_eq!(delivery, *delivery2);
    }
}
