use std::time::Duration;

use anyhow::anyhow;
use async_trait::async_trait;
use bytes::Bytes;
use bytes::BytesMut;
use derivative::Derivative;
use futures03::stream::BoxStream;
use futures03::StreamExt;
use futures03::TryStreamExt;
use graph_derive::CheapClone;
use http::header::CONTENT_LENGTH;
use reqwest::Response;
use reqwest::StatusCode;
use slog::Logger;

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

/// The request that verifies that the IPFS RPC API is accessible is generally fast because
/// it does not involve querying the distributed network.
const TEST_REQUEST_TIMEOUT: Duration = Duration::from_secs(300);

#[derive(Clone, CheapClone, Derivative)]
#[derivative(Debug)]
/// A client that connects to an IPFS RPC API.
///
/// Reference: <https://docs.ipfs.tech/reference/kubo/rpc>
pub struct IpfsRpcClient {
    server_address: ServerAddress,

    #[derivative(Debug = "ignore")]
    http_client: reqwest::Client,

    logger: Logger,
    test_request_timeout: Duration,
}

impl IpfsRpcClient {
    /// Creates a new [IpfsRpcClient] with the specified server address.
    /// Verifies that the server is responding to IPFS RPC API requests.
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

    /// Creates a new [IpfsRpcClient] with the specified server address.
    /// Does not verify that the server is responding to IPFS RPC API requests.
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
        let client = self.to_owned();

        let ok = retry_policy("IPFS.RPC.send_test_request", &self.logger)
            .run(move || {
                let client = client.clone();

                async move {
                    // While there may be unrelated servers that successfully respond to this
                    // request, it is good enough to at least filter out unresponsive servers and
                    // confirm that the server behaves like an IPFS RPC API.
                    let status = client
                        .call("version", Some(client.test_request_timeout))
                        .await?
                        .status();

                    Ok(status == StatusCode::OK)
                }
            })
            .await?;

        if !ok {
            return Err(anyhow!("not an RPC API"));
        }

        Ok(())
    }

    async fn call(
        &self,
        path_and_query: impl AsRef<str>,
        timeout: Option<Duration>,
    ) -> IpfsResult<Response> {
        let url = self.url(path_and_query);
        let mut req = self.http_client.post(url);

        // Some servers require `content-length` even for an empty body.
        req = req.header(CONTENT_LENGTH, 0);

        if let Some(timeout) = timeout {
            req = req.timeout(timeout);
        }

        Ok(req.send().await?.error_for_status()?)
    }

    fn url(&self, path_and_query: impl AsRef<str>) -> String {
        format!("{}api/v0/{}", self.server_address, path_and_query.as_ref())
    }
}

#[async_trait]
impl CanProvide for IpfsRpcClient {
    async fn can_provide(&self, path: &ContentPath, timeout: Option<Duration>) -> IpfsResult<bool> {
        let client = self.to_owned();
        let path = path.to_owned();

        retry_policy("IPFS.RPC.can_provide", &self.logger)
            .run(move || {
                let client = client.clone();
                let path = path.clone();

                async move {
                    let status = client
                        .call(format!("cat?arg={path}&length=1"), timeout)
                        .await?
                        .status();

                    Ok(status == StatusCode::OK)
                }
            })
            .await
    }
}

#[async_trait]
impl CatStream for IpfsRpcClient {
    async fn cat_stream(
        &self,
        path: &ContentPath,
        timeout: Option<Duration>,
    ) -> IpfsResult<BoxStream<'static, IpfsResult<Bytes>>> {
        let client = self.to_owned();
        let path = path.to_owned();

        let resp = retry_policy("IPFS.RPC.cat_stream", &self.logger)
            .run(move || {
                let client = client.clone();
                let path = path.clone();

                async move { Ok(client.call(format!("cat?arg={path}"), timeout).await?) }
            })
            .await?;

        Ok(resp.bytes_stream().err_into().boxed())
    }
}

#[async_trait]
impl Cat for IpfsRpcClient {
    async fn cat(
        &self,
        path: &ContentPath,
        max_size: usize,
        timeout: Option<Duration>,
    ) -> IpfsResult<Bytes> {
        let client = self.to_owned();
        let path = path.to_owned();

        retry_policy("IPFS.RPC.cat", &self.logger)
            .run(move || {
                let client = client.clone();
                let path = path.clone();

                async move {
                    let content = client
                        .call(format!("cat?arg={path}"), timeout)
                        .await?
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
impl GetBlock for IpfsRpcClient {
    async fn get_block(&self, path: &ContentPath, timeout: Option<Duration>) -> IpfsResult<Bytes> {
        let client = self.to_owned();
        let path = path.to_owned();

        retry_policy("IPFS.RPC.get_block", &self.logger)
            .run(move || {
                let client = client.clone();
                let path = path.clone();

                async move {
                    let block = client
                        .call(format!("block/get?arg={path}"), timeout)
                        .await?
                        .bytes()
                        .await?;

                    Ok(block)
                }
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

    const CID: &str = "QmUNLLsPACCz1vLxQVkXqqLX5R1X345qqfHbsf67hvA3Nn";

    async fn mock_server() -> MockServer {
        MockServer::start().await
    }

    fn mock_post(path: &str) -> MockBuilder {
        Mock::given(m::method("POST")).and(m::path(format!("/api/v0/{path}")))
    }

    fn mock_can_provide() -> MockBuilder {
        mock_post("cat")
            .and(m::query_param("arg", CID))
            .and(m::query_param("length", "1"))
    }

    fn mock_cat() -> MockBuilder {
        mock_post("cat").and(m::query_param("arg", CID))
    }

    fn mock_get_block() -> MockBuilder {
        mock_post("block/get").and(m::query_param("arg", CID))
    }

    async fn make_client() -> (MockServer, IpfsRpcClient) {
        let server = mock_server().await;
        let client = IpfsRpcClient::new_unchecked(server.uri(), &discard()).unwrap();

        (server, client)
    }

    fn make_path() -> ContentPath {
        ContentPath::new(CID).unwrap()
    }

    fn ms(millis: u64) -> Duration {
        Duration::from_millis(millis)
    }

    #[tokio::test]
    async fn new_fails_to_create_the_client_if_rpc_api_is_not_accessible() {
        let server = mock_server().await;

        IpfsRpcClient::new(server.uri(), &discard())
            .await
            .unwrap_err();
    }

    #[tokio::test]
    async fn new_creates_the_client_if_it_can_check_the_rpc_api() {
        let server = mock_server().await;

        mock_post("version")
            .respond_with(ResponseTemplate::new(StatusCode::OK))
            .expect(1)
            .mount(&server)
            .await;

        IpfsRpcClient::new(server.uri(), &discard()).await.unwrap();
    }

    #[tokio::test]
    async fn new_retries_rpc_api_check_on_retriable_errors() {
        let server = mock_server().await;

        mock_post("version")
            .respond_with(ResponseTemplate::new(StatusCode::INTERNAL_SERVER_ERROR))
            .up_to_n_times(1)
            .expect(1)
            .mount(&server)
            .await;

        mock_post("version")
            .respond_with(ResponseTemplate::new(StatusCode::OK))
            .expect(1)
            .mount(&server)
            .await;

        IpfsRpcClient::new(server.uri(), &discard()).await.unwrap();
    }

    #[tokio::test]
    async fn new_does_not_retry_rpc_api_check_on_non_retriable_errors() {
        let server = mock_server().await;

        mock_post("version")
            .respond_with(ResponseTemplate::new(StatusCode::METHOD_NOT_ALLOWED))
            .expect(1)
            .mount(&server)
            .await;

        IpfsRpcClient::new(server.uri(), &discard())
            .await
            .unwrap_err();
    }

    #[tokio::test]
    async fn new_unchecked_creates_the_client_without_checking_the_rpc_api() {
        let server = mock_server().await;

        IpfsRpcClient::new_unchecked(server.uri(), &discard()).unwrap();
    }

    #[tokio::test]
    async fn can_provide_returns_true_when_content_is_available() {
        let (server, client) = make_client().await;

        mock_can_provide()
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

        mock_can_provide()
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

        mock_can_provide()
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

        mock_can_provide()
            .respond_with(ResponseTemplate::new(StatusCode::INTERNAL_SERVER_ERROR))
            .up_to_n_times(1)
            .expect(1)
            .mount(&server)
            .await;

        mock_can_provide()
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

        mock_can_provide()
            .respond_with(ResponseTemplate::new(StatusCode::GATEWAY_TIMEOUT))
            .expect(1)
            .mount(&server)
            .await;

        client.can_provide(&make_path(), None).await.unwrap_err();
    }

    #[tokio::test]
    async fn cat_stream_returns_the_content() {
        let (server, client) = make_client().await;

        mock_cat()
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

        assert_eq!(content.as_ref(), b"some data");
    }

    #[tokio::test]
    async fn cat_stream_fails_on_timeout() {
        let (server, client) = make_client().await;

        mock_cat()
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

        mock_cat()
            .respond_with(ResponseTemplate::new(StatusCode::INTERNAL_SERVER_ERROR))
            .up_to_n_times(1)
            .expect(1)
            .mount(&server)
            .await;

        mock_cat()
            .respond_with(ResponseTemplate::new(StatusCode::OK))
            .expect(1)
            .mount(&server)
            .await;

        let _stream = client.cat_stream(&make_path(), None).await.unwrap();
    }

    #[tokio::test]
    async fn cat_stream_does_not_retry_the_request_on_non_retriable_errors() {
        let (server, client) = make_client().await;

        mock_cat()
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

        mock_cat()
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

        mock_cat()
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

        mock_cat()
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

        mock_cat()
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

        mock_cat()
            .respond_with(ResponseTemplate::new(StatusCode::INTERNAL_SERVER_ERROR))
            .up_to_n_times(1)
            .expect(1)
            .mount(&server)
            .await;

        mock_cat()
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

        mock_cat()
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
