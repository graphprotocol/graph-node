use std::sync::Arc;

use anyhow::anyhow;
use async_trait::async_trait;
use futures03::stream::FuturesUnordered;
use futures03::stream::StreamExt;
use slog::Logger;

use crate::ipfs::IpfsClient;
use crate::ipfs::IpfsError;
use crate::ipfs::IpfsRequest;
use crate::ipfs::IpfsResponse;
use crate::ipfs::IpfsResult;

/// Contains a list of IPFS clients and, for each read request, selects the fastest IPFS client
/// that can provide the content and streams the response from that client.
///
/// This can significantly improve performance when using multiple IPFS gateways,
/// as some of them may already have the content cached.
pub struct IpfsClientPool {
    clients: Vec<Arc<dyn IpfsClient>>,
    logger: Logger,
}

impl IpfsClientPool {
    /// Creates a new IPFS client pool from the specified clients.
    pub fn new(clients: Vec<Arc<dyn IpfsClient>>, logger: &Logger) -> Self {
        Self {
            clients,
            logger: logger.to_owned(),
        }
    }
}

#[async_trait]
impl IpfsClient for IpfsClientPool {
    fn logger(&self) -> &Logger {
        &self.logger
    }

    async fn call(self: Arc<Self>, req: IpfsRequest) -> IpfsResult<IpfsResponse> {
        let mut futs = self
            .clients
            .iter()
            .map(|client| client.clone().call(req.clone()))
            .collect::<FuturesUnordered<_>>();

        let mut last_err = None;

        while let Some(result) = futs.next().await {
            match result {
                Ok(resp) => return Ok(resp),
                Err(err) => last_err = Some(err),
            };
        }

        let path = match req {
            IpfsRequest::Cat(path) => path,
            IpfsRequest::GetBlock(path) => path,
        };

        let err = last_err.unwrap_or_else(|| IpfsError::ContentNotAvailable {
            path,
            reason: anyhow!("no clients can provide the content"),
        });

        Err(err)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use bytes::BytesMut;
    use futures03::TryStreamExt;
    use http::StatusCode;
    use wiremock::matchers as m;
    use wiremock::Mock;
    use wiremock::MockBuilder;
    use wiremock::MockServer;
    use wiremock::ResponseTemplate;

    use super::*;
    use crate::ipfs::ContentPath;
    use crate::ipfs::IpfsGatewayClient;
    use crate::ipfs::RetryPolicy;
    use crate::log::discard;

    const PATH: &str = "/ipfs/QmUNLLsPACCz1vLxQVkXqqLX5R1X345qqfHbsf67hvA3Nn";

    fn mock_get() -> MockBuilder {
        Mock::given(m::method("GET")).and(m::path(PATH))
    }

    async fn make_client() -> (MockServer, Arc<IpfsGatewayClient>) {
        let server = MockServer::start().await;
        let client = IpfsGatewayClient::new_unchecked(server.uri(), &discard()).unwrap();

        (server, Arc::new(client))
    }

    fn make_path() -> ContentPath {
        ContentPath::new(PATH).unwrap()
    }

    fn ms(millis: u64) -> Duration {
        Duration::from_millis(millis)
    }

    #[tokio::test]
    async fn cat_stream_streams_the_response_from_the_fastest_client() {
        let (server_1, client_1) = make_client().await;
        let (server_2, client_2) = make_client().await;
        let (server_3, client_3) = make_client().await;

        mock_get()
            .respond_with(
                ResponseTemplate::new(StatusCode::OK)
                    .set_body_bytes(b"server_1")
                    .set_delay(ms(300)),
            )
            .expect(1)
            .mount(&server_1)
            .await;

        mock_get()
            .respond_with(
                ResponseTemplate::new(StatusCode::OK)
                    .set_body_bytes(b"server_2")
                    .set_delay(ms(200)),
            )
            .expect(1)
            .mount(&server_2)
            .await;

        mock_get()
            .respond_with(
                ResponseTemplate::new(StatusCode::OK)
                    .set_body_bytes(b"server_3")
                    .set_delay(ms(100)),
            )
            .expect(1)
            .mount(&server_3)
            .await;

        let clients: Vec<Arc<dyn IpfsClient>> = vec![client_1, client_2, client_3];
        let pool = Arc::new(IpfsClientPool::new(clients, &discard()));

        let bytes = pool
            .cat_stream(&make_path(), None, RetryPolicy::None)
            .await
            .unwrap()
            .try_fold(BytesMut::new(), |mut acc, chunk| async {
                acc.extend(chunk);
                Ok(acc)
            })
            .await
            .unwrap();

        assert_eq!(bytes.as_ref(), b"server_3");
    }

    #[tokio::test]
    async fn cat_streams_the_response_from_the_fastest_client() {
        let (server_1, client_1) = make_client().await;
        let (server_2, client_2) = make_client().await;
        let (server_3, client_3) = make_client().await;

        mock_get()
            .respond_with(
                ResponseTemplate::new(StatusCode::OK)
                    .set_body_bytes(b"server_1")
                    .set_delay(ms(300)),
            )
            .expect(1)
            .mount(&server_1)
            .await;

        mock_get()
            .respond_with(
                ResponseTemplate::new(StatusCode::OK)
                    .set_body_bytes(b"server_2")
                    .set_delay(ms(200)),
            )
            .expect(1)
            .mount(&server_2)
            .await;

        mock_get()
            .respond_with(
                ResponseTemplate::new(StatusCode::OK)
                    .set_body_bytes(b"server_3")
                    .set_delay(ms(100)),
            )
            .expect(1)
            .mount(&server_3)
            .await;

        let clients: Vec<Arc<dyn IpfsClient>> = vec![client_1, client_2, client_3];
        let pool = Arc::new(IpfsClientPool::new(clients, &discard()));

        let bytes = pool
            .cat(&make_path(), usize::MAX, None, RetryPolicy::None)
            .await
            .unwrap();

        assert_eq!(bytes.as_ref(), b"server_3")
    }

    #[tokio::test]
    async fn get_block_streams_the_response_from_the_fastest_client() {
        let (server_1, client_1) = make_client().await;
        let (server_2, client_2) = make_client().await;
        let (server_3, client_3) = make_client().await;

        mock_get()
            .respond_with(
                ResponseTemplate::new(StatusCode::OK)
                    .set_body_bytes(b"server_1")
                    .set_delay(ms(300)),
            )
            .expect(1)
            .mount(&server_1)
            .await;

        mock_get()
            .respond_with(
                ResponseTemplate::new(StatusCode::OK)
                    .set_body_bytes(b"server_2")
                    .set_delay(ms(200)),
            )
            .expect(1)
            .mount(&server_2)
            .await;

        mock_get()
            .respond_with(
                ResponseTemplate::new(StatusCode::OK)
                    .set_body_bytes(b"server_3")
                    .set_delay(ms(100)),
            )
            .expect(1)
            .mount(&server_3)
            .await;

        let clients: Vec<Arc<dyn IpfsClient>> = vec![client_1, client_2, client_3];
        let pool = Arc::new(IpfsClientPool::new(clients, &discard()));

        let bytes = pool
            .get_block(&make_path(), None, RetryPolicy::None)
            .await
            .unwrap();

        assert_eq!(bytes.as_ref(), b"server_3")
    }
}
