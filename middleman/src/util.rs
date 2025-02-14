use crate::types::{ColumnFamilyName, Db, DbColumnFamily};

// Looks up a column family, unsafely rewriting the lifetime to be static.
pub(crate) unsafe fn get_cf(db: &Db, name: ColumnFamilyName) -> DbColumnFamily {
    let name: &'static str = name.into();
    let cf = db.cf_handle(name).unwrap();
    unsafe { std::mem::transmute(cf) }
}

pub fn init_logging() {
    let env = env_logger::Env::new().filter_or("RUST_LOG", "info");
    env_logger::Builder::from_env(env).init();
}
