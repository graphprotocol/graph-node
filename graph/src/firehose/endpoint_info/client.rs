use anyhow::Context;
use anyhow::Result;
use tonic::codec::CompressionEncoding;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;

use super::info_response::InfoResponse;
use crate::firehose::codec;
use crate::firehose::interceptors::AuthInterceptor;
use crate::firehose::interceptors::MetricsInterceptor;

pub struct Client {
    inner: codec::endpoint_info_client::EndpointInfoClient<
        InterceptedService<MetricsInterceptor<Channel>, AuthInterceptor>,
    >,
}

impl Client {
    pub fn new(metrics: MetricsInterceptor<Channel>, auth: AuthInterceptor) -> Self {
        let mut inner =
            codec::endpoint_info_client::EndpointInfoClient::with_interceptor(metrics, auth);

        inner = inner.accept_compressed(CompressionEncoding::Gzip);

        Self { inner }
    }

    pub fn with_compression(mut self) -> Self {
        self.inner = self.inner.send_compressed(CompressionEncoding::Gzip);
        self
    }

    pub fn with_max_message_size(mut self, size: usize) -> Self {
        self.inner = self.inner.max_decoding_message_size(size);
        self
    }

    pub async fn info(&mut self) -> Result<InfoResponse> {
        let req = codec::InfoRequest {};
        let resp = self.inner.info(req).await?.into_inner();

        resp.clone()
            .try_into()
            .with_context(|| format!("received response: {resp:?}"))
    }
}
