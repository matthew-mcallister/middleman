use std::sync::Arc;

use sqlx::{AnyConnection, ColumnIndex, Connection, Row};
use tracing::info;
use uuid::Uuid;

use crate::app::Application;
use crate::config::SqlIngestionOptions;
use crate::error::Result;
use crate::event::EventBuilder;

#[derive(Debug)]
pub struct SqlIngestor {
    app: Arc<Application>,
    connection: AnyConnection,
    table_name: String,
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
        // TOOD: Probably should parse URI to strip out username/password and log the rest
        info!("connecting to ingestion db...");
        sqlx::any::install_default_drivers();
        let connection = AnyConnection::connect(options.url).await?;
        info!("connected to ingestion db");
        Ok(Self {
            app,
            connection,
            table_name: options.table.to_owned(),
        })
    }

    pub async fn consume_events(&mut self) -> Result<()> {
        while self.consume_events_batch().await? {}
        Ok(())
    }

    /// Returns `true` if there are more events to consume
    async fn consume_events_batch(&mut self) -> Result<bool> {
        let limit = 100;

        #[rustfmt::skip]
        let events: Vec<Event> = sqlx::query_as(&format!(stringify!(
            select idempotency_key, tag, stream, payload
            from {}
            limit ?
        ), &self.table_name))
            .bind(limit)
            .fetch_all(&mut self.connection)
            .await?;

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

        // Value list is ?,?,?,...,? with one ? per event
        let mut value_list: String = String::with_capacity(2 * events.len() - 1);
        value_list.push('?');
        for _ in 1..events.len() {
            value_list.push_str(",?");
        }
        #[rustfmt::skip]
        let query = format!(stringify!(
            delete from {}
            where idempotency_key in ({})
        ), &self.table_name, value_list);
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
