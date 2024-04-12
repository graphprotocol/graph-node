use anyhow::Error;
use bytes::Bytes;
use futures::future::BoxFuture;
use graph::{
    data::{subgraph::Link, value::Word},
    http_client::{FileSizeLimit, HttpClient},
    prelude::CheapClone,
};
use std::{sync::Arc, time::Duration};
use tower::{buffer::Buffer, ServiceBuilder, ServiceExt};

const CLOUDFLARE_TIMEOUT: u16 = 524;
const GATEWAY_TIMEOUT: u16 = 504;

pub type HttpService = Buffer<Word, BoxFuture<'static, Result<Option<Bytes>, Error>>>;

pub fn http_service(
    client: Arc<HttpClient>,
    max_file_size: usize,
    timeout: Duration,
    rate_limit: u16,
) -> HttpService {
    let http = HttpServiceInner {
        client,
        max_file_size,
        timeout,
    };

    let svc = ServiceBuilder::new()
        .rate_limit(rate_limit.into(), Duration::from_secs(1))
        .service_fn(move |req| http.cheap_clone().call_inner(req))
        .boxed();

    // The `Buffer` makes it so the rate limit is shared among clones.
    // Make it unbounded to avoid any risk of starvation.
    Buffer::new(svc, u32::MAX as usize)
}

#[derive(Clone)]
struct HttpServiceInner {
    client: Arc<HttpClient>,
    max_file_size: usize,
    timeout: Duration,
}

impl CheapClone for HttpServiceInner {
    fn cheap_clone(&self) -> Self {
        Self {
            client: self.client.cheap_clone(),
            max_file_size: self.max_file_size,
            timeout: self.timeout,
        }
    }
}

impl HttpServiceInner {
    async fn call_inner(self, req: Word) -> Result<Option<Bytes>, Error> {
        let res = self
            .client
            .get_with_limit(
                &Link {
                    link: req.to_string(),
                },
                &FileSizeLimit::MaxBytes(self.max_file_size.try_into().unwrap()),
            )
            .await;

        match res {
            Ok(file_bytes) => Ok(Some(Bytes::from(file_bytes))),
            Err(e) => match e.status().map(|e| e.as_u16()) {
                // Timeouts in IPFS mean the file is not available, so we return `None`
                Some(GATEWAY_TIMEOUT) | Some(CLOUDFLARE_TIMEOUT) => return Ok(None),
                _ if e.is_timeout() => return Ok(None),
                _ => return Err(e.into()),
            },
        }
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;
    use tower::ServiceExt;

    use graph::{data::value::Word, http_client::HttpClient, tokio};

    #[tokio::test]
    async fn cat_file_in_folder() {
        let url = Word::from("https://api.universalprofile.cloud/ipfs/QmaMY4aex2v9QPkxAN1mXQzoNzAY5EyLbpEegN6Mrx5bAa");

        let local = HttpClient::default();

        let svc = super::http_service(local.into(), 100000, Duration::from_secs(5), 10);

        let content = svc.oneshot(url).await.unwrap().unwrap();

        let content: String = content.to_vec().into_iter().map(|b| b as char).collect();

        let comp = r#"#{"LSP4Metadata":{"name":"HexGenzo (Gen0)","description":"HexGenzo marks the migration to a new era, where the iconic hexagon flows to a new sphere without any but liquid boarders.\nAfter the iconic HexJerzo in 2020, this piece is closing a circle and removing any boundaries for a liquefied future.\nDesigned by Schirin Negahbani for The Dematerialised","totalSupply":1214,"images":[[{"width":1080,"height":1080,"url":"ipfs://QmX8v3JTtkmNDwaLZt1Hfav2FPYJpQxFiMzvp6gmDYqQao","verification":{"method":"keccak256(bytes)","data":"0x3b37b6c24d6a4db20b0d6fa5a9c16c4bef7e369e527e21467dd1debf5e20f3e5"}}]],"assets":[{"url":"ipfs://QmaezFf7ZtD1RCkYj2KABEHFmUgMQM6LrVnAu2vZokxHdF","fileType":"video/mp4","verification":{"method":"keccak256(bytes)","data":"0xe9dd95cc67e9cac623d86e5803c67bd78dd39896ff76ad25d5094d1ac4ec3dee"}}],"attributes":[{"value":"Wearable","type":"string","key":"Standard type"}"#;
        assert_eq!(content, comp);
    }
}
