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
}

pub fn load_config() -> Result<Box<Config>> {
    if cfg!(debug_assertions) {
        dotenvy::dotenv().unwrap();
    }
    Ok(Box::new(Config {
        db_dir: std::env::var("MIDDLEMAN_DB_DIR")?.into(),
        host: IpAddr::from_str(
            &std::env::var("MIDDLEMAN_LISTEN").unwrap_or("127.0.0.1".to_owned()),
        )?,
        port: u16::from_str(&std::env::var("MIDDLEMAN_PORT").unwrap_or("10707".to_owned()))?,
    }))
}
