//! HTTP built on hyper.

use std::fmt::Debug;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;

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
use crate::subscriber::SubscriberTable;

#[derive(Clone, Debug, Default)]
struct Resolver;

impl Resolver {
    async fn resolve(&self, host: impl ToSocketAddrs) -> Result<SocketAddr> {
        Ok(
            tokio::net::lookup_host(host).await?.next().ok_or(Error::with_cause(
                ErrorKind::NetworkError,
                "DNS lookup failed",
            ))?,
        )
    }
}

trait Stream: AsyncRead + AsyncWrite + Debug + Send + Sync + Unpin + 'static {}

impl<T> Stream for T where T: AsyncRead + AsyncWrite + Debug + Send + Sync + Unpin + 'static {}

#[derive(Debug)]
pub(crate) struct HttpConnection {
    inner: hyper::client::conn::http1::SendRequest<String>,
    keep_alive_secs: u16,
    // TODO: TCP timeout. Currently we wait infinitely long for a response.
}

impl HttpConnection {
    pub(crate) async fn send_request(
        &mut self,
        mut request: Request<String>,
    ) -> Result<Response<Incoming>> {
        let keep_alive = HeaderValue::from_str(&self.keep_alive_secs.to_string()).unwrap();
        request.headers_mut().insert("Keep-Alive", keep_alive);
        Ok(self.inner.send_request(request).await?)
    }
}

impl Connection for HttpConnection {}

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
    /// Value to set on the HTTP Keep-Alive header. Defaults to 30.
    keep_alive_secs: u16,
}

impl<'a> ConnectionInfo<'a> {
    fn from_url(url: &'a Url) -> Result<Self> {
        let tls = match url.scheme() {
            "http" => false,
            "https" => true,
            _ => Err(ErrorKind::InvalidInput)?,
        };
        let default_port = if tls { 443 } else { 80 };
        let host = url.host().ok_or(ErrorKind::InvalidInput)?;
        Ok(Self {
            host,
            port: url.port().unwrap_or(default_port),
            tls,
            keep_alive_secs: 30,
        })
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

        let (send_request, connection) =
            hyper::client::conn::http1::Builder::new().handshake(TokioIo::new(stream)).await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });

        Ok(HttpConnection {
            inner: send_request,
            keep_alive_secs: info.keep_alive_secs,
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
    type Key = Uuid;
    type Connection = HttpConnection;

    fn connect<'a>(
        &'a self,
        key: &'a Self::Key,
        keep_alive_secs: u16,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Connection>> + Send + 'a>> {
        Box::pin(async move {
            let subscriber = self.subscribers.get(*key)?.ok_or(ErrorKind::InvalidInput)?;
            let url = Url::from_str(subscriber.destination_url()).unwrap();
            let mut info = ConnectionInfo::from_url(&url).unwrap();
            info.keep_alive_secs = keep_alive_secs;
            self.raw.connect(info).await
        })
    }
}

pub(crate) type SubscriberConnectionPool = Http11ConnectionPool<Uuid, HttpConnection>;

/// Fills in the "Timestamp" and "Signature" headers on the request.
pub(crate) fn timestamp_and_sign_request(
    timestamp: chrono::DateTime<FixedOffset>,
    hmac_key: &str,
    request: &mut Request<String>,
) {
    let timestamp = format!("{}", timestamp.format("%+"));
    request.headers_mut().insert("Timestamp", HeaderValue::from_str(&timestamp).unwrap());

    let mut mac = Hmac::<Sha256>::new_from_slice(hmac_key.as_ref()).unwrap();
    mac.update(timestamp.as_bytes());
    mac.update(b"|");
    mac.update(request.body().as_bytes());
    let signature = hex::encode(&mac.finalize().into_bytes());
    request.headers_mut().insert("Signature", HeaderValue::from_str(&signature).unwrap());
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
        println!("{:?}", port);

        let j = tokio::spawn(async move {
            let response = concat!(
                "HTTP/1.1 200 OK\r\n",
                "Content-Type: text/plain\r\n",
                "Content-Length: 15\r\n",
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
                    "keep-alive: 30\r\n",
                    "\r\n",
                ),
            );
            stream.write(response.as_bytes()).await.unwrap();
        });

        let conn_info = ConnectionInfo {
            host: Host::Domain(host),
            port,
            tls: false,
            keep_alive_secs: 30,
        };
        let mut conn = factory.connect(conn_info).await.unwrap();
        let request = Request::get("/").body(String::new()).unwrap();
        let response = conn.send_request(request).await.unwrap();
        assert_eq!(response.status(), 200);
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            &b"Hello, world!\r\n"[..],
        );

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
        assert_eq!(
            sig_header,
            "e280eb411338ec950ac9dc8574c3670cf44d2df79f3e2839e5059b4a7608f820",
        );
    }
}
