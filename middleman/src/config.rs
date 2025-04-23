// XXX: Probably need to support alternative fs layouts at some point

use std::fs::File;
use std::io::Read;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use serde::Deserialize;
use tracing::warn;

use crate::error::{Error, ErrorKind, Result};

const DEFAULT_CONFIG_PATH: &'static str = "/etc/middleman/middlemand.toml";

macro_rules! partial_config {
    ($($field:ident: $Field:ty),*$(,)?) => {
        #[derive(Clone, Debug, Default, Deserialize)]
        struct PartialConfig {
            $($field: $Field,)*
        }

        impl PartialConfig {
            // Rhs takes precedence
            fn union(self, other: Self) -> Self {
                Self {
                    $($field: other.$field.or(self.$field),)*
                }
            }
        }
    }
}

partial_config! {
    data_dir: Option<PathBuf>,
    producer_api_host: Option<IpAddr>,
    producer_api_port: Option<u16>,
    producer_api_bearer_token: Option<String>,
    consumer_api_host: Option<IpAddr>,
    consumer_api_port: Option<u16>,
    consumer_auth_secret: Option<String>,
    ingestion_db_url: Option<String>,
    ingestion_db_table: Option<String>,
}

#[derive(Clone, Debug)]
pub struct Config {
    pub data_dir: PathBuf,
    pub producer_api_host: IpAddr,
    pub producer_api_port: u16,
    /// Optional bearer token to authenticate producer API requests
    pub producer_api_bearer_token: Option<String>,
    pub consumer_api_host: Option<IpAddr>,
    pub consumer_api_port: Option<u16>,
    /// Secret key used to authenticate consumer API auth JWTs
    pub consumer_auth_secret: Option<String>,
    pub ingestion_db_url: Option<String>,
    pub ingestion_db_table: Option<String>,
}

#[derive(Debug)]
pub struct ConsumerApiOptions<'c> {
    pub host: IpAddr,
    pub port: u16,
    pub auth_secret: &'c str,
}

#[derive(Debug)]
pub struct SqlIngestionOptions<'c> {
    pub url: &'c str,
    pub table: &'c str,
}

impl TryFrom<PartialConfig> for Config {
    type Error = Box<Error>;

    fn try_from(value: PartialConfig) -> Result<Self> {
        Ok(Self {
            data_dir: value.data_dir.ok_or(Box::new(Error::with_cause(
                ErrorKind::InvalidInput,
                "missing DATA_DIR".to_owned(),
            )))?,
            producer_api_host: value.producer_api_host.ok_or(Box::new(Error::with_cause(
                ErrorKind::InvalidInput,
                "missing PRODUCER_API_HOST".to_owned(),
            )))?,
            producer_api_port: value.producer_api_port.ok_or(Box::new(Error::with_cause(
                ErrorKind::InvalidInput,
                "missing PRODUCER_API_PORT".to_owned(),
            )))?,
            producer_api_bearer_token: value.producer_api_bearer_token,
            consumer_api_host: value.consumer_api_host,
            consumer_api_port: value.consumer_api_port,
            consumer_auth_secret: value.consumer_auth_secret,
            ingestion_db_url: value.ingestion_db_url,
            ingestion_db_table: value.ingestion_db_table,
        })
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            data_dir: "/var/lib/middleman".to_string().into(),
            producer_api_host: IpAddr::from_str("127.0.0.1").unwrap(),
            producer_api_port: 81,
            producer_api_bearer_token: None,
            consumer_api_host: None,
            consumer_api_port: None,
            consumer_auth_secret: None,
            ingestion_db_url: None,
            ingestion_db_table: None,
        }
    }
}

impl Config {
    pub(crate) fn db_dir(&self) -> PathBuf {
        self.data_dir.join("db")
    }

    pub fn consumer_api_options(&self) -> Option<ConsumerApiOptions> {
        Some(ConsumerApiOptions {
            host: self.consumer_api_host?,
            port: self.consumer_api_port?,
            auth_secret: self.consumer_auth_secret.as_ref()?,
        })
    }

    pub fn sql_ingestion_options(&self) -> Option<SqlIngestionOptions> {
        Some(SqlIngestionOptions {
            url: self.ingestion_db_url.as_ref()?,
            table: self.ingestion_db_table.as_ref()?,
        })
    }
}

fn get_var<T>(var: &str) -> Result<Option<T>>
where
    T: FromStr,
    Box<Error>: From<<T as FromStr>::Err>,
{
    match std::env::var(var) {
        Ok(s) => Ok(Some(FromStr::from_str(&s)?)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(e) => Err(e)?,
    }
}

fn load_from_env() -> Result<PartialConfig> {
    Ok(PartialConfig {
        data_dir: get_var("MIDDLEMAN_DATA_DIR")?,
        producer_api_host: get_var("MIDDLEMAN_PRODUCER_API_HOST")?,
        producer_api_port: get_var("MIDDLEMAN_PRODUCER_API_PORT")?,
        producer_api_bearer_token: get_var("MIDDLEMAN_PRODUCER_API_BEARER_TOKEN")?,
        consumer_api_host: get_var("MIDDLEMAN_CONSUMER_API_HOST")?,
        consumer_api_port: get_var("MIDDLEMAN_CONSUMER_API_PORT")?,
        consumer_auth_secret: get_var("MIDDLEMAN_CONSUMER_AUTH_SECRET")?,
        ingestion_db_url: get_var("MIDDLEMAN_INGESTION_DB_URL")?,
        ingestion_db_table: get_var("MIDDLEMAN_INGESTION_DB_TABLE")?,
    })
}

fn load_from_file(path: &Path) -> Result<PartialConfig> {
    let mut file = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            warn!(?path, "config file not found");
            return Ok(Default::default());
        },
        Err(e) => Err(e)?,
    };
    let mut content = String::new();
    file.read_to_string(&mut content)?;
    Ok(toml::from_str(&content)?)
}

pub fn load_config(mut path: Option<PathBuf>) -> Result<Box<Config>> {
    if path.is_none() {
        path = get_var("MIDDLEMAN_CONFIG_PATH")?;
    }
    let path = path.unwrap_or(DEFAULT_CONFIG_PATH.to_owned().into());
    let config = load_from_file(Path::new(&path))?;
    let config = config.union(load_from_env()?);
    Ok(Box::new(Config::try_from(config)?))
}
