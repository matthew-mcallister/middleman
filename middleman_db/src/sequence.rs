use std::sync::atomic::{AtomicU64, Ordering};

use crate::column_family::ColumnFamily;
use crate::error::Result;

#[derive(Debug)]
pub struct Sequence {
    column_family: ColumnFamily,
    checkpoint_key: Vec<u8>,
    persist_key: Vec<u8>,
    current_value: AtomicU64,
}

// Force a checkpoint after this many elements for crash recovery
const CHECKPOINT_INTERVAL: u64 = 8192;

impl Sequence {
    pub fn new(column_family: ColumnFamily, key: impl Into<Vec<u8>>) -> Result<Self> {
        let key = key.into();

        let mut this = Self {
            column_family,
            checkpoint_key: [b"_middleman_seq_ckpt_", &key[..]].concat(),
            persist_key: [b"_middleman_seq_", &key[..]].concat(),
            current_value: Default::default(),
        };

        let checkpoint_value = this.read_key(&this.checkpoint_key)?;
        let persisted_value = this.read_key(&this.persist_key)?;
        if persisted_value < checkpoint_value {
            // Non-clean shutdown; skip to next checkpoint
            this.current_value = AtomicU64::new(checkpoint_value + CHECKPOINT_INTERVAL);
        } else {
            this.current_value = AtomicU64::new(persisted_value);
        }

        Ok(this)
    }

    fn read_key(&self, key: &[u8]) -> Result<u64> {
        let bytes = self.column_family.get(&key)?;
        if let Some(bytes) = bytes {
            let bytes = <[u8; 8]>::try_from(&bytes[..])?;
            Ok(u64::from_ne_bytes(bytes))
        } else {
            Ok(0)
        }
    }

    pub fn next(&self) -> Result<u64> {
        loop {
            let value = self.current_value.load(Ordering::Acquire);
            if value % CHECKPOINT_INTERVAL == 0 {
                // Block on checkpoint. There is technically a race condition
                // where we can go backwards; not sure if it needs addressing.
                self.column_family.put(&self.checkpoint_key, value.to_ne_bytes())?;
            }
            if let Ok(_) = self.current_value.compare_exchange(
                value,
                value + 1,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                return Ok(value + 1);
            }
        }
    }
}

impl Drop for Sequence {
    fn drop(&mut self) {
        // Can't do anything if this fails
        let _ = self.column_family.put(
            &self.persist_key,
            self.current_value.get_mut().to_ne_bytes(),
        );
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::sequence::CHECKPOINT_INTERVAL;
    use crate::testing::TestDb;

    use super::Sequence;

    #[test]
    fn test_sequence() {
        let mut test_db = TestDb::new();
        let db = test_db.db();
        Arc::get_mut(db)
            .unwrap()
            .create_column_family(&("test", &Default::default(), Default::default()))
            .unwrap();
        let cf = db.get_column_family("test").unwrap();
        let seq = Sequence::new(cf, b"sequence".to_owned()).unwrap();
        assert_eq!(seq.next().unwrap(), 1);
        assert_eq!(seq.next().unwrap(), 2);
        assert_eq!(seq.next().unwrap(), 3);
    }

    #[test]
    fn test_persistence() {
        let mut test_db = TestDb::new();
        let db = test_db.db();
        Arc::get_mut(db)
            .unwrap()
            .create_column_family(&("test", &Default::default(), Default::default()))
            .unwrap();
        let cf = db.get_column_family("test").unwrap();

        let seq = Sequence::new(cf.clone(), b"sequence".to_owned()).unwrap();
        assert_eq!(seq.next().unwrap(), 1);
        drop(seq);

        let seq = Sequence::new(cf.clone(), b"sequence".to_owned()).unwrap();
        assert_eq!(seq.next().unwrap(), 2);
    }

    #[test]
    fn test_checkpoint() {
        let mut test_db = TestDb::new();
        let db = test_db.db();
        Arc::get_mut(db)
            .unwrap()
            .create_column_family(&("test", &Default::default(), Default::default()))
            .unwrap();
        let cf = db.get_column_family("test").unwrap();
        let seq = Sequence::new(cf.clone(), b"sequence".to_owned()).unwrap();

        for _ in 0..CHECKPOINT_INTERVAL + 1 {
            seq.next().unwrap();
        }
        assert_eq!(seq.next().unwrap(), CHECKPOINT_INTERVAL + 2);
    }
}
