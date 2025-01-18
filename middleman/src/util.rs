use crate::{
    error::DynResult,
    types::{Db, DbColumnFamily},
};

// Gets or creates a column family, unsafely rewriting the lifetime to
// be static.
pub(crate) unsafe fn get_or_create_cf(
    db: &Db,
    name: &str,
    options: &rocksdb::Options,
) -> DynResult<DbColumnFamily> {
    let cf = if let Some(cf) = db.cf_handle(name) {
        cf
    } else {
        db.create_cf(name, options)?;
        db.cf_handle(name).unwrap()
    };
    Ok(unsafe { std::mem::transmute(cf) })
}
