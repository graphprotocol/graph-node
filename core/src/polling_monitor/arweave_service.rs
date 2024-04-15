use anyhow::Error;
use bytes::Bytes;
use graph::futures03::future::BoxFuture;
use graph::{
    components::link_resolver::{ArweaveClient, ArweaveResolver, FileSizeLimit},
    data_source::offchain::Base64,
    derive::CheapClone,
    prelude::CheapClone,
};
use std::{sync::Arc, time::Duration};
use tower::{buffer::Buffer, ServiceBuilder, ServiceExt};

pub type ArweaveService = Buffer<Base64, BoxFuture<'static, Result<Option<Bytes>, Error>>>;

pub fn arweave_service(
    client: Arc<ArweaveClient>,
    rate_limit: u16,
    max_file_size: FileSizeLimit,
) -> ArweaveService {
    let arweave = ArweaveServiceInner {
        client,
        max_file_size,
    };

    let svc = ServiceBuilder::new()
        .rate_limit(rate_limit.into(), Duration::from_secs(1))
        .service_fn(move |req| arweave.cheap_clone().call_inner(req))
        .boxed();

    // The `Buffer` makes it so the rate limit is shared among clones.
    // Make it unbounded to avoid any risk of starvation.
    Buffer::new(svc, u32::MAX as usize)
}

#[derive(Clone, CheapClone)]
struct ArweaveServiceInner {
    client: Arc<ArweaveClient>,
    max_file_size: FileSizeLimit,
}

impl ArweaveServiceInner {
    async fn call_inner(self, req: Base64) -> Result<Option<Bytes>, Error> {
        self.client
            .get_with_limit(&req, &self.max_file_size)
            .await
            .map(Bytes::from)
            .map(Some)
            .map_err(Error::from)
    }
}
