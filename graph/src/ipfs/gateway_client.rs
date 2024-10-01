use std::time::Duration;

use anyhow::anyhow;
use async_trait::async_trait;
use bytes::Bytes;
use bytes::BytesMut;
use derivative::Derivative;
use futures03::stream::BoxStream;
use futures03::StreamExt;
use futures03::TryStreamExt;
use http::header::ACCEPT;
use http::header::CACHE_CONTROL;
use reqwest::StatusCode;
use slog::Logger;

use crate::derive::CheapClone;
use crate::ipfs::retry_policy::retry_policy;
use crate::ipfs::CanProvide;
use crate::ipfs::Cat;
use crate::ipfs::CatStream;
use crate::ipfs::ContentPath;
use crate::ipfs::GetBlock;
use crate::ipfs::IpfsClient;
use crate::ipfs::IpfsError;
use crate::ipfs::IpfsResult;
use crate::ipfs::ServerAddress;

/// The request that verifies that the IPFS gateway is accessible is generally fast because
/// it does not involve querying the distributed network.
const TEST_REQUEST_TIMEOUT: Duration = Duration::from_secs(300);

#[derive(Clone, CheapClone, Derivative)]
#[derivative(Debug)]
/// A client that connects to an IPFS gateway.
///
/// Reference: <https://specs.ipfs.tech/http-gateways/path-gateway>
pub struct IpfsGatewayClient {
    server_address: ServerAddress,

    #[derivative(Debug = "ignore")]
    http_client: reqwest::Client,

    logger: Logger,
    test_request_timeout: Duration,
}

impl IpfsGatewayClient {
    /// Creates a new [IpfsGatewayClient] with the specified server address.
    /// Verifies that the server is responding to IPFS gateway requests.
    pub async fn new(server_address: impl AsRef<str>, logger: &Logger) -> IpfsResult<Self> {
        let client = Self::new_unchecked(server_address, logger)?;

        client
            .send_test_request()
            .await
            .map_err(|reason| IpfsError::InvalidServer {
                server_address: client.server_address.clone(),
                reason,
            })?;

        Ok(client)
    }

    /// Creates a new [IpfsGatewayClient] with the specified server address.
    /// Does not verify that the server is responding to IPFS gateway requests.
    pub fn new_unchecked(server_address: impl AsRef<str>, logger: &Logger) -> IpfsResult<Self> {
        Ok(Self {
            server_address: ServerAddress::new(server_address)?,
            http_client: reqwest::Client::new(),
            logger: logger.to_owned(),
            test_request_timeout: TEST_REQUEST_TIMEOUT,
        })
    }

    pub fn into_boxed(self) -> Box<dyn IpfsClient> {
        Box::new(self)
    }

    async fn send_test_request(&self) -> anyhow::Result<()> {
        // To successfully perform this test, it does not really matter which CID we use.
        const RANDOM_CID: &str = "QmUNLLsPACCz1vLxQVkXqqLX5R1X345qqfHbsf67hvA3Nn";

        // A special request described in the specification that should instruct the gateway
        // to perform a very quick local check and return either HTTP status 200, which would
        // mean the server has the content locally cached, or a 412 error, which would mean the
        // content is not locally cached. This information is sufficient to verify that the
        // server behaves like an IPFS gateway.
        let req = self
            .http_client
            .head(self.ipfs_url(RANDOM_CID))
            .header(CACHE_CONTROL, "only-if-cached")
            .timeout(self.test_request_timeout);

        let ok = retry_policy("IPFS.Gateway.send_test_request", &self.logger)
            .run(move || {
                let req = req.try_clone().expect("request can be cloned");

                async move {
                    let resp = req.send().await?;
                    let status = resp.status();

                    if status == StatusCode::OK || status == StatusCode::PRECONDITION_FAILED {
                        return Ok(true);
                    }

                    resp.error_for_status()?;

                    Ok(false)
                }
            })
            .await?;

        if !ok {
            return Err(anyhow!("not a gateway"));
        }

        Ok(())
    }

    fn ipfs_url(&self, path_and_query: impl AsRef<str>) -> String {
        format!("{}ipfs/{}", self.server_address, path_and_query.as_ref())
    }
}

#[async_trait]
impl CanProvide for IpfsGatewayClient {
    async fn can_provide(&self, path: &ContentPath, timeout: Option<Duration>) -> IpfsResult<bool> {
        let url = self.ipfs_url(path.to_string());
        let mut req = self.http_client.head(url);

        if let Some(timeout) = timeout {
            req = req.timeout(timeout);
        }

        retry_policy("IPFS.Gateway.can_provide", &self.logger)
            .run(move || {
                let req = req.try_clone().expect("request can be cloned");

                async move {
                    let status = req.send().await?.error_for_status()?.status();

                    Ok(status == StatusCode::OK)
                }
            })
            .await
    }
}

#[async_trait]
impl CatStream for IpfsGatewayClient {
    async fn cat_stream(
        &self,
        path: &ContentPath,
        timeout: Option<Duration>,
    ) -> IpfsResult<BoxStream<'static, IpfsResult<Bytes>>> {
        let url = self.ipfs_url(path.to_string());
        let mut req = self.http_client.get(url);

        if let Some(timeout) = timeout {
            req = req.timeout(timeout);
        }

        let resp = retry_policy("IPFS.Gateway.cat_stream", &self.logger)
            .run(move || {
                let req = req.try_clone().expect("request can be cloned");

                async move { Ok(req.send().await?.error_for_status()?) }
            })
            .await?;

        Ok(resp.bytes_stream().err_into().boxed())
    }
}

#[async_trait]
impl Cat for IpfsGatewayClient {
    async fn cat(
        &self,
        path: &ContentPath,
        max_size: usize,
        timeout: Option<Duration>,
    ) -> IpfsResult<Bytes> {
        let url = self.ipfs_url(path.to_string());
        let mut req = self.http_client.get(url);

        if let Some(timeout) = timeout {
            req = req.timeout(timeout);
        }

        let path = path.to_owned();

        retry_policy("IPFS.Gateway.cat", &self.logger)
            .run(move || {
                let path = path.clone();
                let req = req.try_clone().expect("request can be cloned");

                async move {
                    let content = req
                        .send()
                        .await?
                        .error_for_status()?
                        .bytes_stream()
                        .err_into()
                        .try_fold(BytesMut::new(), |mut acc, chunk| async {
                            acc.extend(chunk);

                            if acc.len() > max_size {
                                return Err(IpfsError::ContentTooLarge {
                                    path: path.clone(),
                                    max_size,
                                });
                            }

                            Ok(acc)
                        })
                        .await?;

                    Ok(content.into())
                }
            })
            .await
    }
}

#[async_trait]
impl GetBlock for IpfsGatewayClient {
    async fn get_block(&self, path: &ContentPath, timeout: Option<Duration>) -> IpfsResult<Bytes> {
        let url = self.ipfs_url(format!("{path}?format=raw"));

        let mut req = self
            .http_client
            .get(url)
            .header(ACCEPT, "application/vnd.ipld.raw");

        if let Some(timeout) = timeout {
            req = req.timeout(timeout);
        }

        retry_policy("IPFS.Gateway.get_block", &self.logger)
            .run(move || {
                let req = req.try_clone().expect("request can be cloned");

                async move { Ok(req.send().await?.error_for_status()?.bytes().await?) }
            })
            .await
    }
}

#[cfg(test)]
mod tests {
    use wiremock::matchers as m;
    use wiremock::Mock;
    use wiremock::MockBuilder;
    use wiremock::MockServer;
    use wiremock::ResponseTemplate;

    use super::*;
    use crate::log::discard;

    const PATH: &str = "/ipfs/QmUNLLsPACCz1vLxQVkXqqLX5R1X345qqfHbsf67hvA3Nn";

    async fn mock_server() -> MockServer {
        MockServer::start().await
    }

    fn mock_head() -> MockBuilder {
        Mock::given(m::method("HEAD")).and(m::path(PATH))
    }

    fn mock_get() -> MockBuilder {
        Mock::given(m::method("GET")).and(m::path(PATH))
    }

    fn mock_gateway_check(status: StatusCode) -> Mock {
        mock_head()
            .and(m::header("Cache-Control", "only-if-cached"))
            .respond_with(ResponseTemplate::new(status))
    }

    fn mock_get_block() -> MockBuilder {
        mock_get()
            .and(m::query_param("format", "raw"))
            .and(m::header("Accept", "application/vnd.ipld.raw"))
    }

    async fn make_client() -> (MockServer, IpfsGatewayClient) {
        let server = mock_server().await;
        let client = IpfsGatewayClient::new_unchecked(server.uri(), &discard()).unwrap();

        (server, client)
    }

    fn make_path() -> ContentPath {
        ContentPath::new(PATH).unwrap()
    }

    fn ms(millis: u64) -> Duration {
        Duration::from_millis(millis)
    }

    #[tokio::test]
    async fn new_fails_to_create_the_client_if_gateway_is_not_accessible() {
        let server = mock_server().await;

        IpfsGatewayClient::new(server.uri(), &discard())
            .await
            .unwrap_err();
    }

    #[tokio::test]
    async fn new_creates_the_client_if_it_can_check_the_gateway() {
        let server = mock_server().await;

        // Test content is cached locally on the gateway.
        mock_gateway_check(StatusCode::OK)
            .up_to_n_times(1)
            .expect(1)
            .mount(&server)
            .await;

        IpfsGatewayClient::new(server.uri(), &discard())
            .await
            .unwrap();

        // Test content is not cached locally on the gateway.
        mock_gateway_check(StatusCode::PRECONDITION_FAILED)
            .expect(1)
            .mount(&server)
            .await;

        IpfsGatewayClient::new(server.uri(), &discard())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn new_retries_gateway_check_on_retriable_errors() {
        let server = mock_server().await;

        mock_gateway_check(StatusCode::INTERNAL_SERVER_ERROR)
            .up_to_n_times(1)
            .expect(1)
            .mount(&server)
            .await;

        mock_gateway_check(StatusCode::OK)
            .expect(1)
            .mount(&server)
            .await;

        IpfsGatewayClient::new(server.uri(), &discard())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn new_does_not_retry_gateway_check_on_non_retriable_errors() {
        let server = mock_server().await;

        mock_gateway_check(StatusCode::METHOD_NOT_ALLOWED)
            .expect(1)
            .mount(&server)
            .await;

        IpfsGatewayClient::new(server.uri(), &discard())
            .await
            .unwrap_err();
    }

    #[tokio::test]
    async fn new_unchecked_creates_the_client_without_checking_the_gateway() {
        let server = mock_server().await;

        IpfsGatewayClient::new_unchecked(server.uri(), &discard()).unwrap();
    }

    #[tokio::test]
    async fn can_provide_returns_true_when_content_is_available() {
        let (server, client) = make_client().await;

        mock_head()
            .respond_with(ResponseTemplate::new(StatusCode::OK))
            .expect(1)
            .mount(&server)
            .await;

        let ok = client.can_provide(&make_path(), None).await.unwrap();

        assert!(ok);
    }

    #[tokio::test]
    async fn can_provide_returns_false_when_content_is_not_completely_available() {
        let (server, client) = make_client().await;

        mock_head()
            .respond_with(ResponseTemplate::new(StatusCode::PARTIAL_CONTENT))
            .expect(1)
            .mount(&server)
            .await;

        let ok = client.can_provide(&make_path(), None).await.unwrap();

        assert!(!ok);
    }

    #[tokio::test]
    async fn can_provide_fails_on_timeout() {
        let (server, client) = make_client().await;

        mock_head()
            .respond_with(ResponseTemplate::new(StatusCode::OK).set_delay(ms(500)))
            .expect(1)
            .mount(&server)
            .await;

        client
            .can_provide(&make_path(), Some(ms(300)))
            .await
            .unwrap_err();
    }

    #[tokio::test]
    async fn can_provide_retries_the_request_on_retriable_errors() {
        let (server, client) = make_client().await;

        mock_head()
            .respond_with(ResponseTemplate::new(StatusCode::INTERNAL_SERVER_ERROR))
            .up_to_n_times(1)
            .expect(1)
            .mount(&server)
            .await;

        mock_head()
            .respond_with(ResponseTemplate::new(StatusCode::OK))
            .expect(1)
            .mount(&server)
            .await;

        let ok = client.can_provide(&make_path(), None).await.unwrap();

        assert!(ok);
    }

    #[tokio::test]
    async fn can_provide_does_not_retry_the_request_on_non_retriable_errors() {
        let (server, client) = make_client().await;

        mock_head()
            .respond_with(ResponseTemplate::new(StatusCode::GATEWAY_TIMEOUT))
            .expect(1)
            .mount(&server)
            .await;

        client.can_provide(&make_path(), None).await.unwrap_err();
    }

    #[tokio::test]
    async fn cat_stream_returns_the_content() {
        let (server, client) = make_client().await;

        mock_get()
            .respond_with(ResponseTemplate::new(StatusCode::OK).set_body_bytes(b"some data"))
            .expect(1)
            .mount(&server)
            .await;

        let content = client
            .cat_stream(&make_path(), None)
            .await
            .unwrap()
            .try_fold(BytesMut::new(), |mut acc, chunk| async {
                acc.extend(chunk);

                Ok(acc)
            })
            .await
            .unwrap();

        assert_eq!(content.as_ref(), b"some data")
    }

    #[tokio::test]
    async fn cat_stream_fails_on_timeout() {
        let (server, client) = make_client().await;

        mock_get()
            .respond_with(ResponseTemplate::new(StatusCode::OK).set_delay(ms(500)))
            .expect(1)
            .mount(&server)
            .await;

        let result = client.cat_stream(&make_path(), Some(ms(300))).await;

        assert!(matches!(result, Err(_)));
    }

    #[tokio::test]
    async fn cat_stream_retries_the_request_on_retriable_errors() {
        let (server, client) = make_client().await;

        mock_get()
            .respond_with(ResponseTemplate::new(StatusCode::INTERNAL_SERVER_ERROR))
            .up_to_n_times(1)
            .expect(1)
            .mount(&server)
            .await;

        mock_get()
            .respond_with(ResponseTemplate::new(StatusCode::OK))
            .expect(1)
            .mount(&server)
            .await;

        let _stream = client.cat_stream(&make_path(), None).await.unwrap();
    }

    #[tokio::test]
    async fn cat_stream_does_not_retry_the_request_on_non_retriable_errors() {
        let (server, client) = make_client().await;

        mock_get()
            .respond_with(ResponseTemplate::new(StatusCode::GATEWAY_TIMEOUT))
            .expect(1)
            .mount(&server)
            .await;

        let result = client.cat_stream(&make_path(), None).await;

        assert!(matches!(result, Err(_)));
    }

    #[tokio::test]
    async fn cat_returns_the_content() {
        let (server, client) = make_client().await;

        mock_get()
            .respond_with(ResponseTemplate::new(StatusCode::OK).set_body_bytes(b"some data"))
            .expect(1)
            .mount(&server)
            .await;

        let content = client.cat(&make_path(), usize::MAX, None).await.unwrap();

        assert_eq!(content.as_ref(), b"some data");
    }

    #[tokio::test]
    async fn cat_returns_the_content_if_max_size_is_equal_to_the_content_size() {
        let (server, client) = make_client().await;

        let data = b"some data";

        mock_get()
            .respond_with(ResponseTemplate::new(StatusCode::OK).set_body_bytes(data))
            .expect(1)
            .mount(&server)
            .await;

        let content = client.cat(&make_path(), data.len(), None).await.unwrap();

        assert_eq!(content.as_ref(), data);
    }

    #[tokio::test]
    async fn cat_fails_if_content_is_too_large() {
        let (server, client) = make_client().await;

        let data = b"some data";

        mock_get()
            .respond_with(ResponseTemplate::new(StatusCode::OK).set_body_bytes(data))
            .expect(1)
            .mount(&server)
            .await;

        client
            .cat(&make_path(), data.len() - 1, None)
            .await
            .unwrap_err();
    }

    #[tokio::test]
    async fn cat_fails_on_timeout() {
        let (server, client) = make_client().await;

        mock_get()
            .respond_with(ResponseTemplate::new(StatusCode::OK).set_delay(ms(500)))
            .expect(1)
            .mount(&server)
            .await;

        client
            .cat(&make_path(), usize::MAX, Some(ms(300)))
            .await
            .unwrap_err();
    }

    #[tokio::test]
    async fn cat_retries_the_request_on_retriable_errors() {
        let (server, client) = make_client().await;

        mock_get()
            .respond_with(ResponseTemplate::new(StatusCode::INTERNAL_SERVER_ERROR))
            .up_to_n_times(1)
            .expect(1)
            .mount(&server)
            .await;

        mock_get()
            .respond_with(ResponseTemplate::new(StatusCode::OK).set_body_bytes(b"some data"))
            .expect(1)
            .mount(&server)
            .await;

        let content = client.cat(&make_path(), usize::MAX, None).await.unwrap();

        assert_eq!(content.as_ref(), b"some data");
    }

    #[tokio::test]
    async fn cat_does_not_retry_the_request_on_non_retriable_errors() {
        let (server, client) = make_client().await;

        mock_get()
            .respond_with(ResponseTemplate::new(StatusCode::GATEWAY_TIMEOUT))
            .expect(1)
            .mount(&server)
            .await;

        client
            .cat(&make_path(), usize::MAX, None)
            .await
            .unwrap_err();
    }

    #[tokio::test]
    async fn get_block_returns_the_block_content() {
        let (server, client) = make_client().await;

        mock_get_block()
            .respond_with(ResponseTemplate::new(StatusCode::OK).set_body_bytes(b"some data"))
            .expect(1)
            .mount(&server)
            .await;

        let block = client.get_block(&make_path(), None).await.unwrap();

        assert_eq!(block.as_ref(), b"some data");
    }

    #[tokio::test]
    async fn get_block_fails_on_timeout() {
        let (server, client) = make_client().await;

        mock_get_block()
            .respond_with(ResponseTemplate::new(StatusCode::OK).set_delay(ms(500)))
            .expect(1)
            .mount(&server)
            .await;

        client
            .get_block(&make_path(), Some(ms(300)))
            .await
            .unwrap_err();
    }

    #[tokio::test]
    async fn get_block_retries_the_request_on_retriable_errors() {
        let (server, client) = make_client().await;

        mock_get_block()
            .respond_with(ResponseTemplate::new(StatusCode::INTERNAL_SERVER_ERROR))
            .up_to_n_times(1)
            .expect(1)
            .mount(&server)
            .await;

        mock_get_block()
            .respond_with(ResponseTemplate::new(StatusCode::OK).set_body_bytes(b"some data"))
            .expect(1)
            .mount(&server)
            .await;

        let block = client.get_block(&make_path(), None).await.unwrap();

        assert_eq!(block.as_ref(), b"some data");
    }

    #[tokio::test]
    async fn get_block_does_not_retry_the_request_on_non_retriable_errors() {
        let (server, client) = make_client().await;

        mock_get_block()
            .respond_with(ResponseTemplate::new(StatusCode::GATEWAY_TIMEOUT))
            .expect(1)
            .mount(&server)
            .await;

        client.get_block(&make_path(), None).await.unwrap_err();
    }
}
