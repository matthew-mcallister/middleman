#[derive(Clone, Copy, Debug, Eq, IntoStaticStr, PartialEq, VariantNames)]
pub(crate) enum ColumnFamilyName {
    Deliveries,
    Events,
    EventTagIdempotencyKeyIndex,
    EventTagStreamIndex,
    Subscribers,
}

impl db::ColumnFamilyId for ColumnFamilyName {
    /// Returns the descriptor needed to create/open this CF.
    fn descriptor(self) -> (&'static str, rocksdb::Options, rocksdb::ColumnFamilyTtl) {
        let (options, ttl) = match self {
            Self::EventTagStreamIndex => {
                let mut options = rocksdb::Options::default();
                options.set_comparator("big_tuple", Box::new(big_tuple_comparator));
                (options, Default::default())
            },
            _ => Default::default(),
        };
        let name: &'static str = self.into();
        (name, options, ttl)
    }
}
