//! HTTP built on hyper.

use std::fmt::Debug;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use chrono::FixedOffset;
use hmac::{Hmac, Mac};
use http::HeaderValue;
use hyper::body::Incoming;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use sha2::Sha256;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::ToSocketAddrs;
use url::{Host, Url};
use uuid::Uuid;

use crate::connection::{Connection, ConnectionFactory, Http11ConnectionPool};
use crate::error::{Error, ErrorKind, Result};
use crate::subscriber::{Subscriber, SubscriberTable};

// TODO: This should be a config variable
const MAX_KEEPALIVE_SECS: u16 = 60;

#[derive(Clone, Debug, Default)]
struct Resolver;

impl Resolver {
    async fn resolve(&self, host: impl ToSocketAddrs) -> Result<SocketAddr> {
        let mut hosts = tokio::net::lookup_host(host).await?;
        let address = hosts.find(|a| a.is_ipv4());
        Ok(address.ok_or(Error::with_cause(ErrorKind::NetworkError, "DNS lookup failed"))?)
    }
}

trait Stream: AsyncRead + AsyncWrite + Debug + Send + Sync + Unpin + 'static {}

impl<T> Stream for T where T: AsyncRead + AsyncWrite + Debug + Send + Sync + Unpin + 'static {}

fn get_keep_alive_timeout_from_response<B>(response: &Response<B>) -> Result<Option<u16>> {
    let Some(header) = response.headers().get("Keep-Alive") else { return Ok(None) };
    let s = header.to_str()?;
    let params = s.split(",");
    for p in params {
        let mut parts = p.split("=");
        let Some(key) = parts.next() else { continue };
        let Some(value) = parts.next() else { continue };
        if parts.next() != None {
            // ambiguous parse
            Err(ErrorKind::InvalidInput)?
        };
        if key.trim() == "timeout" {
            return Ok(Some(u16::from_str(value.trim())?));
        }
    }
    Ok(None)
}

#[derive(Debug)]
pub(crate) struct HttpConnection {
    inner: hyper::client::conn::http1::SendRequest<String>,
    // XXX: If server doesn't return Keep-Alive, maybe don't pool the
    // connection?
    keep_alive_secs: u16,
}

impl HttpConnection {
    pub(crate) async fn send_request(
        &mut self,
        mut request: Request<String>,
    ) -> Result<Response<Incoming>> {
        request.headers_mut().insert("Connection", "keep-alive".try_into().unwrap());
        let timeout = Duration::from_secs(30);
        let response = tokio::time::timeout(timeout, self.inner.send_request(request)).await??;
        // XXX: Should we continue to ignore invalid Keep-Alive?
        if let Ok(Some(keep_alive)) = get_keep_alive_timeout_from_response(&response) {
            self.keep_alive_secs = std::cmp::min(keep_alive, MAX_KEEPALIVE_SECS);
        }
        Ok(response)
    }
}

impl Connection for HttpConnection {
    fn keep_alive(&self) -> u16 {
        self.keep_alive_secs
    }
}

#[derive(Debug)]
struct RawConnectionFactory {
    resolver: Resolver,
    tls_connector: tokio_native_tls::TlsConnector,
}

#[derive(Debug)]
struct ConnectionInfo<'a> {
    host: Host<&'a str>,
    port: u16,
    tls: bool,
}

impl<'a> ConnectionInfo<'a> {
    fn from_url(url: &'a Url) -> Result<Self> {
        let tls = match url.scheme() {
            "http" => false,
            "https" => true,
            _ => Err(ErrorKind::InvalidInput)?,
        };
        let default_port = if tls { 443 } else { 80 };
        let port = url.port().unwrap_or(default_port);
        let host = url.host().ok_or(ErrorKind::InvalidInput)?;
        Ok(Self { host, port, tls })
    }
}

impl RawConnectionFactory {
    pub(crate) fn new() -> Result<Self> {
        let tls_connector = tokio_native_tls::native_tls::TlsConnector::builder().build()?;
        Ok(Self {
            resolver: Default::default(),
            tls_connector: tls_connector.into(),
        })
    }

    async fn connect(&self, info: ConnectionInfo<'_>) -> Result<HttpConnection> {
        let host = info.host.to_string();
        let address = self.resolver.resolve((&host[..], info.port)).await?;
        let stream = tokio::net::TcpStream::connect(address).await?;
        let stream: Box<dyn Stream> = if info.tls {
            Box::new(self.tls_connector.connect(&host, stream).await?)
        } else {
            Box::new(stream)
        };
        let stream = Pin::new(stream);

        let (send_request, connection) = hyper::client::conn::http1::Builder::new()
            .handshake(TokioIo::new(stream))
            .await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });

        Ok(HttpConnection {
            inner: send_request,
            keep_alive_secs: 30,
        })
    }
}

#[derive(Debug)]
pub(crate) struct SubscriberConnectionFactory {
    raw: RawConnectionFactory,
    subscribers: Arc<SubscriberTable>,
}

impl SubscriberConnectionFactory {
    pub(crate) fn new(subscribers: Arc<SubscriberTable>) -> Result<Self> {
        Ok(Self {
            raw: RawConnectionFactory::new()?,
            subscribers,
        })
    }
}

impl ConnectionFactory for SubscriberConnectionFactory {
    type Key = (Uuid, Uuid);
    type Connection = HttpConnection;

    fn max_connections(&self, key: &Self::Key) -> Result<u16> {
        Ok(if let Some(subscriber) = self.subscribers.get(key.0, key.1)? {
            subscriber.max_connections()
        } else {
            // TODO: This should be a config variable
            Subscriber::default_max_connections()
        })
    }

    fn connect<'a>(
        &'a self,
        key: &'a Self::Key,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Connection>> + Send + 'a>> {
        Box::pin(async move {
            let subscriber = self.subscribers.get(key.0, key.1)?.ok_or(ErrorKind::Unexpected)?;
            let url = Url::from_str(subscriber.destination_url()).unwrap();
            let info = ConnectionInfo::from_url(&url).unwrap();
            self.raw.connect(info).await
        })
    }
}

pub(crate) type SubscriberConnectionPool = Http11ConnectionPool<(Uuid, Uuid), HttpConnection>;

/// Fills in the "Timestamp" and "Signature" headers on the request.
pub(crate) fn timestamp_and_sign_request(
    timestamp: chrono::DateTime<FixedOffset>,
    hmac_key: &str,
    request: &mut Request<String>,
) {
    let timestamp = format!("{}", timestamp.format("%+"));
    request
        .headers_mut()
        .insert("Timestamp", HeaderValue::from_str(&timestamp).unwrap());

    let mut mac = Hmac::<Sha256>::new_from_slice(hmac_key.as_ref()).unwrap();
    mac.update(timestamp.as_bytes());
    mac.update(b"|");
    mac.update(request.body().as_bytes());
    let signature = hex::encode(&mac.finalize().into_bytes());
    request
        .headers_mut()
        .insert("Signature", HeaderValue::from_str(&signature).unwrap());
}

#[cfg(test)]
mod tests {
    use http_body_util::BodyExt;
    use hyper::Request;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use url::Host;

    use crate::http::{timestamp_and_sign_request, RawConnectionFactory};

    use super::ConnectionInfo;

    #[tokio::test(flavor = "current_thread")]
    async fn test_raw() {
        let factory = RawConnectionFactory::new().unwrap();

        let host = "127.0.0.1";
        let listener = tokio::net::TcpListener::bind((host, 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();

        let j = tokio::spawn(async move {
            let response = concat!(
                "HTTP/1.1 200 OK\r\n",
                "Content-Type: text/plain\r\n",
                "Content-Length: 15\r\n",
                "Keep-Alive: max=200, timeout=15\r\n",
                "\r\n",
                "Hello, world!\r\n"
            );
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::<u8>::new();
            while request.len() < 4 || &request[request.len() - 4..] != b"\r\n\r\n" {
                stream.read_buf(&mut request).await.unwrap();
            }
            #[rustfmt::skip]
            assert_eq!(
                std::str::from_utf8(&request).unwrap(),
                concat!(
                    "GET / HTTP/1.1\r\n",
                    "connection: keep-alive\r\n",
                    "\r\n",
                ),
            );
            stream.write_all(response.as_bytes()).await.unwrap();
        });

        let conn_info = ConnectionInfo {
            host: Host::Domain(host),
            port,
            tls: false,
        };
        let mut conn = factory.connect(conn_info).await.unwrap();
        let request = Request::get("/").body(String::new()).unwrap();
        let response = conn.send_request(request).await.unwrap();
        assert_eq!(response.status(), 200);
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            &b"Hello, world!\r\n"[..],
        );
        assert_eq!(conn.keep_alive_secs, 15);

        j.await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_signing() {
        let body = "Hello, world!";
        let mut request = Request::new(body.to_owned());

        let key = "thereisnocowlevel";
        let timestamp = "2025-03-01T12:34:56+00:00";
        let ts = chrono::DateTime::parse_from_rfc3339(timestamp).unwrap();
        timestamp_and_sign_request(ts, key, &mut request);

        let ts_header = request.headers().get("Timestamp").unwrap().to_str().unwrap();
        assert_eq!(ts_header, timestamp);

        let sig_header = request.headers().get("Signature").unwrap().to_str().unwrap();
        assert_eq!(sig_header, "e280eb411338ec950ac9dc8574c3670cf44d2df79f3e2839e5059b4a7608f820",);
    }
}
