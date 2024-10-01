use std::time::Duration;

use anyhow::anyhow;
use async_trait::async_trait;
use bytes::Bytes;
use futures03::stream::BoxStream;
use futures03::stream::FuturesUnordered;
use futures03::stream::StreamExt;

use crate::ipfs::CanProvide;
use crate::ipfs::Cat;
use crate::ipfs::CatStream;
use crate::ipfs::ContentPath;
use crate::ipfs::GetBlock;
use crate::ipfs::IpfsClient;
use crate::ipfs::IpfsError;
use crate::ipfs::IpfsResult;

/// Contains a list of IPFS clients and, for each read request, selects the fastest IPFS client
/// that can provide the content and forwards the request to that client.
///
/// This can significantly improve performance when using multiple IPFS gateways,
/// as some of them may already have the content cached.
///
/// Note: It should remain an implementation detail and not be used directly.
pub(super) struct IpfsClientPool {
    inner: Vec<Box<dyn IpfsClient>>,
}

impl IpfsClientPool {
    pub(super) fn with_clients(clients: Vec<Box<dyn IpfsClient>>) -> Self {
        Self { inner: clients }
    }

    pub(super) fn into_boxed(self) -> Box<dyn IpfsClient> {
        Box::new(self)
    }
}

#[async_trait]
impl CanProvide for IpfsClientPool {
    async fn can_provide(&self, path: &ContentPath, timeout: Option<Duration>) -> IpfsResult<bool> {
        select_fastest_ipfs_client(&self.inner, path, timeout)
            .await
            .map(|_client| true)
    }
}

#[async_trait]
impl CatStream for IpfsClientPool {
    async fn cat_stream(
        &self,
        path: &ContentPath,
        timeout: Option<Duration>,
    ) -> IpfsResult<BoxStream<'static, IpfsResult<Bytes>>> {
        let client = select_fastest_ipfs_client(&self.inner, path, timeout).await?;

        client.cat_stream(path, timeout).await
    }
}

#[async_trait]
impl Cat for IpfsClientPool {
    async fn cat(
        &self,
        path: &ContentPath,
        max_size: usize,
        timeout: Option<Duration>,
    ) -> IpfsResult<Bytes> {
        let client = select_fastest_ipfs_client(&self.inner, path, timeout).await?;

        client.cat(path, max_size, timeout).await
    }
}

#[async_trait]
impl GetBlock for IpfsClientPool {
    async fn get_block(&self, path: &ContentPath, timeout: Option<Duration>) -> IpfsResult<Bytes> {
        let client = select_fastest_ipfs_client(&self.inner, path, timeout).await?;

        client.get_block(path, timeout).await
    }
}

/// Returns the first IPFS client that can provide the content from the specified path.
async fn select_fastest_ipfs_client<'a>(
    clients: &'a [Box<dyn IpfsClient>],
    path: &ContentPath,
    timeout: Option<Duration>,
) -> IpfsResult<&'a dyn IpfsClient> {
    let mut futs = clients
        .iter()
        .enumerate()
        .map(|(i, client)| async move {
            client
                .can_provide(path, timeout)
                .await
                .map(|ok| ok.then_some(i))
        })
        .collect::<FuturesUnordered<_>>();

    let mut last_err = None;

    while let Some(result) = futs.next().await {
        match result {
            Ok(Some(i)) => return Ok(clients[i].as_ref()),
            Ok(None) => continue,
            Err(err) => last_err = Some(err),
        };
    }

    let err = last_err.unwrap_or_else(|| IpfsError::ContentNotAvailable {
        path: path.to_owned(),
        reason: anyhow!("no clients can provide the content"),
    });

    Err(err)
}

#[cfg(test)]
mod tests {
    use http::StatusCode;
    use wiremock::matchers as m;
    use wiremock::Mock;
    use wiremock::MockBuilder;
    use wiremock::MockServer;
    use wiremock::ResponseTemplate;

    use super::*;
    use crate::ipfs::IpfsGatewayClient;
    use crate::log::discard;

    const PATH: &str = "/ipfs/QmUNLLsPACCz1vLxQVkXqqLX5R1X345qqfHbsf67hvA3Nn";

    fn mock_head() -> MockBuilder {
        Mock::given(m::method("HEAD")).and(m::path(PATH))
    }

    fn mock_get() -> MockBuilder {
        Mock::given(m::method("GET")).and(m::path(PATH))
    }

    async fn make_client() -> (MockServer, IpfsGatewayClient) {
        let server = MockServer::start().await;
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
    async fn can_provide_returns_true_if_any_client_can_provide_the_content() {
        let (server_1, client_1) = make_client().await;
        let (server_2, client_2) = make_client().await;

        mock_head()
            .respond_with(
                ResponseTemplate::new(StatusCode::INTERNAL_SERVER_ERROR).set_delay(ms(100)),
            )
            .expect(1..)
            .mount(&server_1)
            .await;

        mock_head()
            .respond_with(ResponseTemplate::new(StatusCode::OK).set_delay(ms(200)))
            .expect(1)
            .mount(&server_2)
            .await;

        let clients = vec![client_1.into_boxed(), client_2.into_boxed()];
        let pool = IpfsClientPool::with_clients(clients);
        let ok = pool.can_provide(&make_path(), None).await.unwrap();

        assert!(ok);
    }

    #[tokio::test]
    async fn cat_stream_forwards_the_request_to_the_fastest_client_that_can_provide_the_content() {
        let (server_1, client_1) = make_client().await;
        let (server_2, client_2) = make_client().await;

        mock_head()
            .respond_with(ResponseTemplate::new(StatusCode::OK).set_delay(ms(200)))
            .expect(1)
            .mount(&server_1)
            .await;

        mock_head()
            .respond_with(ResponseTemplate::new(StatusCode::OK).set_delay(ms(100)))
            .expect(1)
            .mount(&server_2)
            .await;

        mock_get()
            .respond_with(ResponseTemplate::new(StatusCode::OK))
            .expect(1)
            .mount(&server_2)
            .await;

        let clients = vec![client_1.into_boxed(), client_2.into_boxed()];
        let pool = IpfsClientPool::with_clients(clients);
        let _stream = pool.cat_stream(&make_path(), None).await.unwrap();
    }

    #[tokio::test]
    async fn cat_forwards_the_request_to_the_fastest_client_that_can_provide_the_content() {
        let (server_1, client_1) = make_client().await;
        let (server_2, client_2) = make_client().await;

        mock_head()
            .respond_with(ResponseTemplate::new(StatusCode::OK).set_delay(ms(200)))
            .expect(1)
            .mount(&server_1)
            .await;

        mock_head()
            .respond_with(ResponseTemplate::new(StatusCode::OK).set_delay(ms(100)))
            .expect(1)
            .mount(&server_2)
            .await;

        mock_get()
            .respond_with(ResponseTemplate::new(StatusCode::OK).set_body_bytes(b"some data"))
            .expect(1)
            .mount(&server_2)
            .await;

        let clients = vec![client_1.into_boxed(), client_2.into_boxed()];
        let pool = IpfsClientPool::with_clients(clients);
        let content = pool.cat(&make_path(), usize::MAX, None).await.unwrap();

        assert_eq!(content.as_ref(), b"some data")
    }

    #[tokio::test]
    async fn get_block_forwards_the_request_to_the_fastest_client_that_can_provide_the_content() {
        let (server_1, client_1) = make_client().await;
        let (server_2, client_2) = make_client().await;

        mock_head()
            .respond_with(ResponseTemplate::new(StatusCode::OK).set_delay(ms(200)))
            .expect(1)
            .mount(&server_1)
            .await;

        mock_head()
            .respond_with(ResponseTemplate::new(StatusCode::OK).set_delay(ms(100)))
            .expect(1)
            .mount(&server_2)
            .await;

        mock_get()
            .respond_with(ResponseTemplate::new(StatusCode::OK).set_body_bytes(b"some data"))
            .expect(1)
            .mount(&server_2)
            .await;

        let clients = vec![client_1.into_boxed(), client_2.into_boxed()];
        let pool = IpfsClientPool::with_clients(clients);
        let block = pool.get_block(&make_path(), None).await.unwrap();

        assert_eq!(block.as_ref(), b"some data")
    }
}
