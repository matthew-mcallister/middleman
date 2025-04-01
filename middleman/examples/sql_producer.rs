use std::time::Duration;

use clap::{Args, Parser, Subcommand};
use rand::Rng;
use sqlx::pool::PoolConnection;
use sqlx::{AnyPool, Executor};
use statrs::distribution::{Chi, ContinuousCDF, DiscreteCDF, Geometric};
use tracing::info;
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
    pool: AnyPool,
    connection: PoolConnection<sqlx::Any>,
}

impl Common {
    async fn new() -> Result<Self> {
        let database_uri = std::env::var("MIDDLEMAN_INGESTION_DB_URL").unwrap();
        let pool = AnyPool::connect(&database_uri).await?;
        let connection = pool.acquire().await?;
        Ok(Self { pool, connection })
    }
}

async fn init_db(mut com: Common) -> Result<()> {
    #[rustfmt::skip]
    com.connection.execute(r"
        create table events (
            idempotency_key blob primary key,
            tag blob,
            stream text,
            payload text
        )
    ").await?;
    Ok(())
}

async fn run(mut com: Common, args: &RunArgs) -> Result<()> {
    let mut rng = rand::rng();
    let dist = Chi::new(3)?;
    let mean = 1.595769121605731;
    let period = 1.0 / args.frequency;
    let tag = uuid!("8d51a639-1d2f-46a4-9fdd-7730860b1990");

    let user_id_dist = Geometric::new(10e-3)?;

    loop {
        let idempotency_key = Uuid::new_v4();
        let user_id = user_id_dist.inverse_cdf(rng.random::<f64>());
        let stream = format!("user:{}", user_id);
        let payload = format!("{{\"data\":{}}}", rng.random::<u16>());
        #[rustfmt::skip]
        sqlx::query::<sqlx::Any>(r"
            insert into events (idempotency_key, tag, stream, payload)
            values (?, ?, ?, ?)
        ")
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
