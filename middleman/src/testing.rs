use std::future::Future;
use std::net::IpAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use compact_str::CompactString;
use http::{Request, Response};
use http_body_util::BodyExt;
use hyper_util::rt::TokioIo;
use tracing::info;

use crate::api::auth::Claims;
use crate::api::consumer::router as consumer_router;
use crate::api::producer::router as producer_router;
use crate::config::{Config, SqlIngestionOptions};
use crate::connection::{Connection, ConnectionFactory, Http11ConnectionPool, Key};
use crate::error::{Error, Result};
use crate::ingestion::sql::SqlIngestor;
use crate::Application;

#[derive(Default)]
pub(crate) struct TestHarness {
    pub(crate) config: Option<Box<Config>>,
    pub(crate) data_dir: Option<tempfile::TempDir>,
    pub(crate) application: Option<Arc<Application>>,
    pub(crate) connection_pool: Option<Http11ConnectionPool<CompactString, TestConnection>>,
    pub(crate) sqlite_db_url: Option<String>,
    pub(crate) sqlite_db: Option<sqlx::SqliteConnection>,
    pub(crate) sql_ingestor: Option<SqlIngestor>,
    pub(crate) producer_api_url: Option<String>,
    pub(crate) consumer_api_url: Option<String>,
}

const CONSUMER_AUTH_SECRET: &'static str = "test_key";

impl TestHarness {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn data_dir(&mut self) -> &std::path::Path {
        if self.data_dir.is_some() {
            return self.data_dir.as_ref().map(|d| d.path()).unwrap();
        }

        // Create temp dir
        let system_temp_dir = std::env::temp_dir();
        let root = PathBuf::from(system_temp_dir).join("middleman");
        match std::fs::create_dir(&root) {
            Ok(_) => {},
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {},
            _ => panic!(),
        }

        self.data_dir = Some(tempfile::tempdir_in(&root).unwrap());
        self.data_dir.as_ref().map(|d| d.path()).unwrap()
    }

    pub fn config(&mut self) -> &mut Box<Config> {
        if self.config.is_some() {
            return self.config.as_mut().unwrap();
        }

        let data_dir = self.data_dir().to_owned();
        let config = Box::new(Config {
            data_dir: data_dir,
            producer_api_host: IpAddr::from_str("127.0.0.1").unwrap(),
            producer_api_port: 8081,
            producer_api_bearer_token: None,
            consumer_api_host: Some(IpAddr::from_str("127.0.0.1").unwrap()),
            consumer_api_port: Some(8080),
            consumer_auth_secret: Some(CONSUMER_AUTH_SECRET.to_owned()),
            ingestion_db_url: None,
            ingestion_db_table: None,
        });
        self.config = Some(config);
        self.config.as_mut().unwrap()
    }

    pub fn application(&mut self) -> &Arc<Application> {
        if self.application.is_some() {
            return self.application.as_mut().unwrap();
        }

        self.application = Some(Arc::new(Application::new(self.config().clone()).unwrap()));
        self.application.as_ref().unwrap()
    }

    pub fn connection_pool(&mut self) -> &mut Http11ConnectionPool<CompactString, TestConnection> {
        if self.connection_pool.is_some() {
            return self.connection_pool.as_mut().unwrap();
        }

        let pool = Http11ConnectionPool::new(
            Default::default(),
            Box::new(TestConnectionFactory::default()),
        );
        self.connection_pool = Some(pool);
        self.connection_pool.as_mut().unwrap()
    }

    pub fn sqlite_db_url(&mut self) -> &str {
        if self.sqlite_db_url.is_some() {
            return self.sqlite_db_url.as_ref().unwrap();
        }

        let data_dir = self.data_dir();
        let path = data_dir.join("test.sqlite").to_str().unwrap().to_owned();
        let url = format!("sqlite://{path}");
        self.sqlite_db_url = Some(url);
        self.sqlite_db_url.as_ref().unwrap()
    }

    pub async fn sqlite_db(&mut self) -> &mut sqlx::SqliteConnection {
        use sqlx::Connection;
        if self.sqlite_db.is_some() {
            return self.sqlite_db.as_mut().unwrap();
        }
        let options = sqlx::sqlite::SqliteConnectOptions::from_str(self.sqlite_db_url())
            .unwrap()
            .create_if_missing(true);
        let mut connection = sqlx::SqliteConnection::connect_with(&options).await.unwrap();

        #[rustfmt::skip]
        sqlx::query(r"
            create table events (
                idempotency_key blob primary key,
                tag blob,
                stream text,
                payload text
            )
        ")
            .execute(&mut connection)
            .await
            .unwrap();

        self.sqlite_db = Some(connection);
        self.sqlite_db.as_mut().unwrap()
    }

    pub async fn sql_ingestor(&mut self) -> &mut SqlIngestor {
        if self.sql_ingestor.is_some() {
            return self.sql_ingestor.as_mut().unwrap();
        }
        let app = Arc::clone(self.application());
        let url = self.sqlite_db_url();
        let options = SqlIngestionOptions {
            url,
            table: "events",
        };
        self.sql_ingestor = Some(SqlIngestor::new(app, options).await.unwrap());
        self.sql_ingestor.as_mut().unwrap()
    }

    pub async fn producer_api(&mut self) -> &str {
        if self.producer_api_url.is_some() {
            return self.producer_api_url.as_ref().unwrap().as_ref();
        }

        let app = self.application();
        let router = producer_router(Arc::clone(app));

        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        info!("Listening on {}", addr);

        tokio::spawn(async { axum::serve(listener, router).await.unwrap() });

        self.producer_api_url = Some(format!("http://{addr}"));
        self.producer_api_url.as_ref().unwrap()
    }

    pub async fn consumer_api(&mut self) -> &str {
        if self.consumer_api_url.is_some() {
            return self.consumer_api_url.as_ref().unwrap().as_ref();
        }

        let app = self.application();
        let router = consumer_router(Arc::clone(app)).unwrap();

        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        info!("Listening on {}", addr);

        tokio::spawn(async { axum::serve(listener, router).await.unwrap() });

        self.consumer_api_url = Some(format!("http://{addr}"));
        self.consumer_api_url.as_ref().unwrap()
    }

    pub fn consumer_auth_token(&self, subject: String) -> String {
        let key = jsonwebtoken::EncodingKey::from_secret(CONSUMER_AUTH_SECRET.as_bytes());
        let claims = Claims { sub: subject };
        jsonwebtoken::encode(&Default::default(), &claims, &key).unwrap()
    }
}

#[derive(Clone, Debug)]
pub struct TestConnection {
    pub num_connections: Arc<AtomicU64>,
    pub id: u64,
    pub host: CompactString,
    pub keep_alive_secs: u16,
}

#[derive(Debug)]
pub struct TestConnectionFactory {
    // Also doubles as total number of connections created
    pub id: AtomicU64,
    pub num_connections: Arc<AtomicU64>,
    pub max_connections_per_host: u16,
}

impl Default for TestConnectionFactory {
    fn default() -> Self {
        Self {
            id: Default::default(),
            num_connections: Default::default(),
            max_connections_per_host: 16,
        }
    }
}

impl TestConnection {
    pub fn host_string(&self) -> &str {
        &self.host[..]
    }
}

impl Drop for TestConnection {
    fn drop(&mut self) {
        self.num_connections.fetch_sub(1, Ordering::Relaxed);
    }
}

impl Key for CompactString {}

impl Connection for TestConnection {
    fn keep_alive(&self) -> u16 {
        30
    }
}

impl ConnectionFactory for TestConnectionFactory {
    type Key = CompactString;
    type Connection = TestConnection;

    fn max_connections(&self, _key: &Self::Key) -> Result<u16> {
        Ok(self.max_connections_per_host)
    }

    fn connect<'a>(
        &'a self,
        key: &'a Self::Key,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Connection>> + Send + 'a>> {
        let num_connections = Arc::clone(&self.num_connections);
        num_connections.fetch_add(1, Ordering::Relaxed);
        let id = self.id.fetch_add(1, Ordering::Relaxed);
        let host = key.clone();
        Box::pin(async move {
            Ok(TestConnection {
                num_connections,
                id,
                host,
                keep_alive_secs: 60,
            })
        })
    }
}

// Used in some http unit tests
pub(crate) async fn http_server<F, S>(f: F) -> Result<(u16, impl Future<Output: Send> + Send)>
where
    S: Future<Output = Result<Response<String>>> + Send,
    F: Fn(Request<String>) -> S + Clone + Send,
{
    let host = "127.0.0.1";
    let listener = tokio::net::TcpListener::bind((host, 0)).await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let responder = async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else { return };
            let g = f.clone();
            let hyper_service =
                hyper::service::service_fn(move |req: Request<hyper::body::Incoming>| {
                    let h = g.clone();
                    async move {
                        let (parts, body) = req.into_parts();
                        let body =
                            std::str::from_utf8(&body.collect().await?.to_bytes())?.to_owned();
                        let request = Request::from_parts(parts, body);
                        let response = h(request).await?;
                        Ok::<_, Box<Error>>(response)
                    }
                });
            let conn = hyper::server::conn::http1::Builder::new()
                .serve_connection(TokioIo::new(stream), hyper_service);
            conn.await.unwrap();
        }
    };
    Ok((port, responder))
}

#[macro_export]
macro_rules! check_status {
    ($status:expr, $request:expr$(,)?) => {
        async {
            loop {
                let response = $request.send().await.unwrap();
                assert_eq!(response.status().as_u16(), $status);
                if let Some(h) = response.headers().get("Content-Type") {
                    if h.as_bytes() == b"application/json" {
                        let body: serde_json::Value =
                            serde_json::from_str(&response.text().await.unwrap()).unwrap();
                        break Some(body);
                    }
                }
                break None;
            }
        }
        .await
    };
}

pub(crate) use check_status;
