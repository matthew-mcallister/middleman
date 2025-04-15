use std::net::IpAddr;
use std::path::PathBuf;
use std::str::FromStr;

use crate::error::Result;

/// Config that changes only after startup/reload.
// TODO: Load from file
#[derive(Clone, Debug)]
pub struct Config {
    pub db_dir: PathBuf,
    pub host: IpAddr,
    pub port: u16,
    pub ingestion_db_url: Option<String>,
    pub ingestion_db_table: Option<String>,
    pub producer_api_bearer_token: Option<String>,
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
        producer_api_bearer_token: std::env::var("MIDDLEMAN_PRODUCER_API_BEARER_TOKEN").ok(),
    }))
}

#[derive(Debug)]
pub struct SqlIngestionOptions<'c> {
    pub url: &'c str,
    pub table: &'c str,
}

impl Config {
    pub fn sql_ingestion_options(&self) -> Option<SqlIngestionOptions> {
        Some(SqlIngestionOptions {
            url: self.ingestion_db_url.as_ref()?,
            table: self.ingestion_db_table.as_ref()?,
        })
    }
}
