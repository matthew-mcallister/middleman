use std::fs::{read_to_string, write};
use std::iter::FusedIterator;
use std::ops::RangeInclusive;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use db::{Db, DbOptions};
use log::info;
use middleman_db as db;

use crate::db::ColumnFamilyName;
use crate::error::{Error, Result};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum Version {
    // V0 means the database hasn't been initialized
    V0 = 0,
    V1 = 1,
}

impl TryFrom<u32> for Version {
    type Error = Box<Error>;

    fn try_from(value: u32) -> std::result::Result<Self, Self::Error> {
        Ok(match value {
            0 => Self::V0,
            1 => Self::V1,
            _ => Err("Invalid version num")?,
        })
    }
}

impl FromStr for Version {
    type Err = Box<Error>;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let int = u32::from_str(s)?;
        Self::try_from(int)
    }
}

impl Version {
    fn next(self) -> Option<Self> {
        Self::try_from(self as u32 + 1).ok()
    }

    fn iter_inclusive(range: RangeInclusive<Self>) -> impl Iterator<Item = Self> {
        assert!(range.start() <= range.end());

        #[derive(Clone, Copy, Debug)]
        struct Iter {
            start: Version,
            end: Version,
            done: bool,
        }

        impl Iterator for Iter {
            type Item = Version;

            fn next(&mut self) -> Option<Self::Item> {
                if self.done {
                    None
                } else if self.start == self.end {
                    self.done = true;
                    Some(self.start)
                } else {
                    let value = self.start;
                    self.start = value.next().unwrap();
                    Some(value)
                }
            }
        }

        impl FusedIterator for Iter {}

        Iter {
            start: *range.start(),
            end: *range.end(),
            done: false,
        }
    }

    fn column_families(self) -> &'static [ColumnFamilyName] {
        match self {
            Self::V0 => &[],
            Self::V1 => &[
                ColumnFamilyName::Meta,
                ColumnFamilyName::Deliveries,
                ColumnFamilyName::DeliveryNextAttemptIndex,
                ColumnFamilyName::Subscribers,
                ColumnFamilyName::Events,
                ColumnFamilyName::EventTagIdempotencyKeyIndex,
                ColumnFamilyName::EventTagStreamIndex,
            ],
        }
    }
}

fn version_file_path(db_dir: &Path) -> PathBuf {
    db_dir.join(".version")
}

fn read_version(db_dir: &Path) -> Result<Version> {
    let path = version_file_path(db_dir);
    Ok(match read_to_string(path) {
        Ok(s) => Version::from_str(&s)?,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Version::V0,
        Err(e) => Err(e)?,
    })
}

fn create_or_open(db_dir: &Path) -> Result<(Version, Db)> {
    let version = read_version(db_dir)?;

    let descriptors =
        Version::iter_inclusive(Version::V0..=version).flat_map(|v| v.column_families()).cloned();

    let mut options = DbOptions::default();
    options.create_if_missing = true;
    let db = Db::open(&db_dir, &options, descriptors)?;

    Ok((version, db))
}

pub(crate) struct Migrator {
    db: Db,
    version: Version,
}

impl Migrator {
    pub(crate) fn new(db_dir: &Path) -> Result<Self> {
        let (version, db) = create_or_open(db_dir)?;

        Ok(Self { db, version })
    }

    fn write_version(&mut self, version: Version) -> Result<()> {
        self.version = version;
        let path = version_file_path(self.db.path());
        let version = (version as u32).to_string();
        write(path, version)?;
        Ok(())
    }

    fn log_migration(&self, version: Version) {
        let version_num = version as u32;
        info!("Migrating to DB version {}", version_num);
    }

    pub(crate) fn migrate(&mut self) -> Result<()> {
        while let Some(next_version) = self.version.next() {
            self.log_migration(next_version);
            for cf in next_version.column_families() {
                self.db.create_column_family(cf)?;
            }
            self.write_version(next_version)?;
        }
        Ok(())
    }

    pub(crate) fn unwrap(self) -> Db {
        self.db
    }
}
