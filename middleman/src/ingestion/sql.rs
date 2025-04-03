use std::fmt::Write;
use std::str::FromStr;
use std::sync::Arc;

use sqlx::{AnyConnection, ColumnIndex, Connection, Row};
use tracing::info;
use url::Url;
use uuid::Uuid;

use crate::app::Application;
use crate::config::SqlIngestionOptions;
use crate::error::Result;
use crate::event::EventBuilder;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Flavor {
    Mysql,
    Postgresql,
}

impl FromStr for Flavor {
    type Err = Box<crate::error::Error>;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        Ok(match s {
            "sqlite" | "mysql" | "mariadb" => Self::Mysql,
            "postgres" | "postgresql" => Self::Postgresql,
            _ => Err("unsupported database")?,
        })
    }
}

#[derive(Debug)]
pub struct SqlIngestor {
    app: Arc<Application>,
    connection: AnyConnection,
    table_name: String,
    flavor: Flavor,
}

#[derive(Debug)]
struct Event {
    idempotency_key: Uuid,
    tag: Uuid,
    stream: String,
    payload: String,
}

fn uuid_try_from(bytes: &[u8]) -> std::result::Result<Uuid, sqlx::Error> {
    let bytes =
        <[u8; 16]>::try_from(bytes).map_err(|_| sqlx::Error::Decode("invalid uuid".into()))?;
    Ok(Uuid::from_bytes(bytes))
}

impl<'r, R: Row> sqlx::FromRow<'r, R> for Event
where
    &'r [u8]: sqlx::Decode<'r, R::Database>,
    String: sqlx::Decode<'r, R::Database>,
    usize: ColumnIndex<R>,
{
    fn from_row(row: &'r R) -> std::result::Result<Self, sqlx::Error> {
        let idempotency_key: &[u8] = row.try_get_unchecked(0)?;
        let tag: &[u8] = row.try_get_unchecked(1)?;
        Ok(Self {
            idempotency_key: uuid_try_from(idempotency_key)?,
            tag: uuid_try_from(tag)?,
            stream: row.try_get_unchecked(2)?,
            payload: row.try_get_unchecked(3)?,
        })
    }
}

impl SqlIngestor {
    pub async fn new(app: Arc<Application>, options: SqlIngestionOptions<'_>) -> Result<Self> {
        let mut url = Url::parse(options.url)?;
        if url.password().is_some() {
            let _ = url.set_password(Some("REDACTED"));
        }
        let flavor = Flavor::from_str(url.scheme())?;
        let url = url.to_string();
        info!(?url, "connecting to ingestion db...");
        sqlx::any::install_default_drivers();
        let connection = AnyConnection::connect(options.url).await?;
        info!(?url, "connected to ingestion db");
        Ok(Self {
            app,
            connection,
            table_name: options.table.to_owned(),
            flavor,
        })
    }

    pub async fn consume_events(&mut self) -> Result<()> {
        while self.consume_events_batch().await? {}
        Ok(())
    }

    /// Returns `true` if there are more events to consume
    async fn consume_events_batch(&mut self) -> Result<bool> {
        let limit = 100;

        let query = match self.flavor {
            Flavor::Postgresql => format!(
                concat!(
                    "select ",
                    "decode(replace(cast(idempotency_key as text), '-', ''), 'hex'), ",
                    "decode(replace(cast(tag as text), '-', ''), 'hex'), ",
                    "stream, payload from {} limit {}",
                ),
                self.table_name, limit,
            ),
            Flavor::Mysql => format!(
                "select idempotency_key, tag, stream, payload from {} limit {}",
                self.table_name, limit,
            ),
        };
        let events: Vec<Event> = sqlx::query_as(&query).fetch_all(&mut self.connection).await?;

        if events.is_empty() {
            return Ok(false);
        }

        for event in events.iter() {
            let mut builder = EventBuilder::new();
            builder
                .idempotency_key(event.idempotency_key)
                .tag(event.tag)
                .stream(&event.stream)
                .payload(&event.payload);
            self.app.create_event(builder)?;
        }

        // delete from <TABLE> where idempotency_key in (?,?,...,?)
        let uuid_list = match self.flavor {
            Flavor::Mysql => {
                let mut list = "?".to_owned();
                for _ in 1..events.len() {
                    write!(list, ",?").unwrap();
                }
                list
            },
            Flavor::Postgresql => {
                let mut list = String::new();
                for i in 0..events.len() {
                    if !list.is_empty() {
                        list.push(',')
                    }
                    write!(list, "cast(encode(${}, 'hex') as uuid)", i + 1).unwrap();
                }
                list
            },
        };
        let query = format!(
            "delete from {} where idempotency_key in ({})",
            self.table_name, uuid_list
        );
        let mut q = sqlx::query(&query);
        for event in events.iter() {
            q = q.bind(&event.idempotency_key.as_bytes()[..]);
        }
        q.execute(&mut self.connection).await?;

        Ok(events.len() == 100)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use uuid::uuid;

    use crate::testing::TestHarness;

    #[tokio::test(flavor = "current_thread")]
    async fn test_ingestion() {
        let mut harness = TestHarness::new();
        let application = Arc::clone(harness.application());

        let idempotency_key = uuid!("602295f7-0ee3-45fa-bd6e-d48883c8ec25");
        let tag = uuid!("246baca1-385b-478b-8513-1b727f79ef7d");
        #[rustfmt::skip]
        sqlx::query(r"
            insert into events (idempotency_key, tag, stream, payload)
            values (?, ?, ?, ?)
        ")
            .bind(&idempotency_key.as_bytes()[..])
            .bind(&tag.as_bytes()[..])
            .bind("strem")
            .bind("1234")
            .execute(harness.sqlite_db().await)
            .await
            .unwrap();

        let ingestor = harness.sql_ingestor().await;
        ingestor.consume_events().await.unwrap();

        let mut txn = application.db.begin_transaction();
        let event = application
            .events
            .get_by_idempotency_key(&mut txn, tag, idempotency_key)
            .unwrap()
            .unwrap();
        assert_eq!(event.stream(), "strem");
        assert_eq!(event.payload(), "1234");
    }
}
