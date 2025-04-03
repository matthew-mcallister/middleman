use std::str::FromStr;
use std::time::Duration;

use clap::{Args, Parser, Subcommand};
use middleman::ingestion::sql::Flavor;
use rand::Rng;
use sqlx::pool::PoolConnection;
use sqlx::{AnyPool, Executor};
use statrs::distribution::{Chi, ContinuousCDF, DiscreteCDF, Geometric};
use tracing::info;
use url::Url;
use uuid::{uuid, Uuid};

type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
type Result<T> = std::result::Result<T, Error>;

#[derive(Parser)]
#[command(version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Init,
    Run(RunArgs),
}

#[derive(Args)]
struct RunArgs {
    #[arg(default_value_t = 1.0)]
    frequency: f64,
}

#[derive(Debug)]
struct Common {
    db_url: Url,
    _pool: AnyPool,
    connection: PoolConnection<sqlx::Any>,
}

impl Common {
    async fn new() -> Result<Self> {
        let db_url = std::env::var("MIDDLEMAN_INGESTION_DB_URL").unwrap();
        let pool = AnyPool::connect(&db_url).await?;
        let db_url = Url::parse(&db_url).unwrap();
        let connection = pool.acquire().await?;
        Ok(Self {
            db_url,
            _pool: pool,
            connection,
        })
    }

    fn flavor(&self) -> Result<Flavor> {
        Ok(Flavor::from_str(self.db_url.scheme())?)
    }
}

fn insert_query(flavor: Flavor) -> &'static str {
    match flavor {
        Flavor::Mysql => stringify!(
            insert into events (idempotency_key, tag, stream, payload)
            values (?, ?, ?, ?)
        ),
        Flavor::Postgresql => concat!(
            "insert into events (idempotency_key, tag, stream, payload) values ",
            "(cast(encode($1, 'hex') as uuid), cast(encode($2, 'hex') as uuid), $3, $4)",
        ),
    }
}

async fn init_db(mut com: Common) -> Result<()> {
    #[rustfmt::skip]
    match com.flavor()? {
        Flavor::Mysql => {
            com.connection.execute(stringify!(
                create table events (
                    idempotency_key blob primary key,
                    tag blob,
                    stream text,
                    payload text
                )
            )).await?;
        },
        Flavor::Postgresql => {
            com.connection.execute(stringify!(
                create table events (
                    idempotency_key uuid primary key,
                    tag uuid,
                    stream text,
                    payload text
                )
            )).await?;
        },
    };
    Ok(())
}

async fn run(mut com: Common, args: &RunArgs) -> Result<()> {
    let mut rng = rand::rng();
    let dist = Chi::new(3)?;
    let mean = 1.595769121605731;
    let period = 1.0 / args.frequency;
    let tag = uuid!("8d51a639-1d2f-46a4-9fdd-7730860b1990");

    let user_id_dist = Geometric::new(10e-3)?;
    let flavor = Flavor::from_str(com.db_url.scheme())?;

    loop {
        let idempotency_key = Uuid::new_v4();
        let user_id = user_id_dist.inverse_cdf(rng.random::<f64>());
        let stream = format!("user:{}", user_id);
        let payload = format!("{{\"data\":{}}}", rng.random::<u16>());
        #[rustfmt::skip]
        sqlx::query::<sqlx::Any>(insert_query(flavor))
            .bind(&idempotency_key.as_bytes()[..])
            .bind(&tag.as_bytes()[..])
            .bind(stream)
            .bind(payload)
            .execute(&mut *com.connection)
            .await?;
        info!("Created event {}", idempotency_key);

        let delay = dist.inverse_cdf(rng.random::<f64>()) / mean * period;
        tokio::time::sleep(Duration::from_secs_f64(delay)).await;
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    dotenvy::dotenv().unwrap();
    sqlx::any::install_default_drivers();
    tracing_subscriber::fmt().with_max_level(tracing::Level::DEBUG).init();

    let com = Common::new().await?;

    match &cli.command {
        Commands::Init => init_db(com).await?,
        Commands::Run(args) => run(com, args).await?,
    }

    Ok(())
}
