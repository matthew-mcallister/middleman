//! HTTP built on hyper.

use std::fmt::Debug;
use std::net::SocketAddr;
use std::pin::Pin;

use hyper::body::Incoming;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::ToSocketAddrs;

use crate::error::{Error, ErrorKind, Result};

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
struct ConnectionInfo<'a> {
    host: &'a str,
    port: u16,
    tls: bool,
}

#[derive(Debug)]
pub(crate) struct Http11ConnectionFactory {
    resolver: Resolver,
    tls_connector: tokio_native_tls::TlsConnector,
}

impl Http11ConnectionFactory {
    pub(crate) fn new() -> Result<Self> {
        let tls_connector = tokio_native_tls::native_tls::TlsConnector::builder().build()?;
        Ok(Self {
            resolver: Default::default(),
            tls_connector: tls_connector.into(),
        })
    }

    async fn connect(
        &self,
        info: ConnectionInfo<'_>,
    ) -> Result<(HttpConnectionHandle, HttpConnection)> {
        let address = self.resolver.resolve((info.host, info.port)).await?;
        let stream = tokio::net::TcpStream::connect(address).await?;
        let stream: Box<dyn Stream> = if info.tls {
            Box::new(self.tls_connector.connect(info.host, stream).await?)
        } else {
            Box::new(stream)
        };
        let stream = Pin::new(stream);

        let (send_request, connection) =
            hyper::client::conn::http1::Builder::new().handshake(TokioIo::new(stream)).await?;
        let handle = tokio::spawn(async move {
            let _ = connection.await;
        });

        let pooled = HttpConnectionHandle { inner: handle };
        let connection = HttpConnection {
            inner: send_request,
        };

        Ok((pooled, connection))
    }
}

/// Handle to the connection service task; terminates the connection when
/// dropped.
#[derive(Debug)]
pub(crate) struct HttpConnectionHandle {
    inner: tokio::task::JoinHandle<()>,
}

impl Drop for HttpConnectionHandle {
    fn drop(&mut self) {
        // This closes the channel
        self.inner.abort();
    }
}

/// External handle to the underlying connection for sending/receiving
/// requests.
#[derive(Debug)]
pub(crate) struct HttpConnection {
    inner: hyper::client::conn::http1::SendRequest<String>,
}

impl HttpConnection {
    pub(crate) async fn send_request(
        &mut self,
        request: Request<String>,
    ) -> Result<Response<Incoming>> {
        Ok(self.inner.send_request(request).await?)
    }
}

#[cfg(test)]
mod tests {
    use http_body_util::BodyExt;
    use hyper::Request;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::{ConnectionInfo, Http11ConnectionFactory};

    #[tokio::test(flavor = "current_thread")]
    async fn test_request() {
        let factory = Http11ConnectionFactory::new().unwrap();

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
                println!("{:?}", request);
            }
            assert_eq!(request, b"GET / HTTP/1.1\r\n\r\n");
            stream.write(response.as_bytes()).await.unwrap();
        });

        let conn_info = ConnectionInfo {
            host,
            port,
            tls: false,
        };
        let (_handle, mut conn) = factory.connect(conn_info).await.unwrap();
        let request = Request::get("/").body(String::new()).unwrap();
        let response = conn.send_request(request).await.unwrap();
        assert_eq!(response.status(), 200);
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            &b"Hello, world!\r\n"[..],
        );

        j.await.unwrap();
    }
}
