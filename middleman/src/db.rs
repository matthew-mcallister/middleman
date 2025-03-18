use middleman_db as db;
use middleman_db::big_tuple::big_tuple_comparator;
use strum_macros::{IntoStaticStr, VariantNames};

#[derive(Clone, Copy, Debug, Eq, IntoStaticStr, PartialEq, VariantNames)]
pub(crate) enum ColumnFamilyName {
    Meta,
    Deliveries,
    DeliveryNextAttemptIndex,
    Events,
    EventTagIdempotencyKeyIndex,
    EventTagStreamIndex,
    Subscribers,
    SubscriberTagIndex,
}

impl db::column_family::ColumnFamilyDescriptor for ColumnFamilyName {
    fn name(&self) -> &str {
        self.into()
    }

    fn options(&self) -> rocksdb::Options {
        match self {
            Self::EventTagStreamIndex => {
                let mut options = rocksdb::Options::default();
                options.set_comparator(
                    "big_tuple",
                    Box::new(|a, b| unsafe { big_tuple_comparator(a, b) }),
                );
                options
            },
            _ => Default::default(),
        }
    }

    fn ttl(&self) -> rocksdb::ColumnFamilyTtl {
        match self {
            _ => Default::default(),
        }
    }
}
