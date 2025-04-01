use std::net::IpAddr;
use std::path::PathBuf;
use std::str::FromStr;

use crate::error::Result;

/// Config that changes only after startup/reload.
// XXX: Make multi-threaded
#[derive(Clone, Debug)]
pub struct Config {
    pub db_dir: PathBuf,
    pub host: IpAddr,
    pub port: u16,
    pub ingestion_db_url: Option<String>,
    pub ingestion_db_table: Option<String>,
}

pub fn load_config() -> Result<Box<Config>> {
    Ok(Box::new(Config {
        db_dir: std::env::var("MIDDLEMAN_DB_DIR")?.into(),
        host: IpAddr::from_str(
            &std::env::var("MIDDLEMAN_LISTEN").unwrap_or("127.0.0.1".to_owned()),
        )?,
        port: u16::from_str(&std::env::var("MIDDLEMAN_PORT").unwrap_or("10707".to_owned()))?,
        ingestion_db_url: std::env::var("MIDDLEMAN_INGESTION_DB_URL").ok(),
        ingestion_db_table: std::env::var("MIDDLEMAN_INGESTION_DB_TABLE").ok(),
    }))
}

#[derive(Debug)]
pub struct IngestionDbOptions<'c> {
    pub url: &'c str,
    pub table: &'c str,
}

impl Config {
    pub fn ingestion_db_options(&self) -> Option<IngestionDbOptions> {
        Some(IngestionDbOptions {
            url: self.ingestion_db_url.as_ref()?,
            table: self.ingestion_db_table.as_ref()?,
        })
    }
}
