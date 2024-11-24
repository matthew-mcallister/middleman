use std::error::Error;
use std::path::PathBuf;

/// Config that changes only after startup/reload.
// XXX: Make multi-threaded
#[derive(Debug)]
pub struct Config {
    pub db_dir: PathBuf,
}

pub fn load_config() -> Result<Config, Box<dyn Error>> {
    if cfg!(debug_assertions) {
        dotenvy::dotenv().unwrap();
    }
    Ok(Config {
        db_dir: std::env::var("db_dir")?.into(),
    })
}
