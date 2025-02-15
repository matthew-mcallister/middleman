use std::sync::Arc;

use owning_ref::OwningRef;

use crate::types::{ColumnFamilyName, Db, DbColumnFamily};

// Looks up a column family, unsafely rewriting the lifetime to be static.
pub(crate) fn get_cf(db: Arc<Db>, name: ColumnFamilyName) -> DbColumnFamily {
    let name: &'static str = name.into();
    OwningRef::new(db).map(|db| db.cf_handle(name).unwrap())
}

pub fn init_logging() {
    let env = env_logger::Env::new().filter_or("RUST_LOG", "info");
    env_logger::Builder::from_env(env).init();
}
