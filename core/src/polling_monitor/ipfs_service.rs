use anyhow::{anyhow, Error};
use bytes::Bytes;
use futures::{Future, FutureExt};
use graph::{
    cheap_clone::CheapClone,
    ipfs_client::{CidFile, IpfsClient, StatApi},
    tokio::sync::Semaphore,
};
use std::{pin::Pin, sync::Arc, task::Poll, time::Duration};
use tower::Service;

const CLOUDFLARE_TIMEOUT: u16 = 524;
const GATEWAY_TIMEOUT: u16 = 504;

/// Reference type, clones will refer to the same service.
#[derive(Clone)]
pub struct IpfsService {
    client: IpfsClient,
    max_file_size: u64,
    timeout: Duration,
    concurrency_limiter: Arc<Semaphore>,
}

impl CheapClone for IpfsService {
    fn cheap_clone(&self) -> Self {
        Self {
            client: self.client.cheap_clone(),
            max_file_size: self.max_file_size,
            timeout: self.timeout,
            concurrency_limiter: self.concurrency_limiter.cheap_clone(),
        }
    }
}

impl IpfsService {
    pub fn new(
        client: IpfsClient,
        max_file_size: u64,
        timeout: Duration,
        concurrency_limit: u16,
    ) -> Self {
        Self {
            client,
            max_file_size,
            timeout,
            concurrency_limiter: Arc::new(Semaphore::new(concurrency_limit as usize)),
        }
    }

    async fn call(&self, req: &CidFile) -> Result<Option<Bytes>, Error> {
        let CidFile { cid, path } = req;
        let multihash = cid.hash().code();
        if !SAFE_MULTIHASHES.contains(&multihash) {
            return Err(anyhow!("CID multihash {} is not allowed", multihash));
        }

        let cid_str = match path {
            Some(path) => format!("{}/{}", cid, path),
            None => cid.to_string(),
        };

        let size = match self
            .client
            .stat_size(StatApi::Files, cid_str.clone(), self.timeout)
            .await
        {
            Ok(size) => size,
            Err(e) => match e.status().map(|e| e.as_u16()) {
                Some(GATEWAY_TIMEOUT) | Some(CLOUDFLARE_TIMEOUT) => return Ok(None),
                _ if e.is_timeout() => return Ok(None),
                _ => return Err(e.into()),
            },
        };

        if size > self.max_file_size {
            return Err(anyhow!(
                "IPFS file {} is too large. It can be at most {} bytes but is {} bytes",
                cid_str,
                self.max_file_size,
                size
            ));
        }

        Ok(self
            .client
            .cat_all(&cid_str, self.timeout)
            .await
            .map(Some)?)
    }
}

impl Service<CidFile> for IpfsService {
    type Response = (CidFile, Option<Bytes>);
    type Error = (CidFile, Error);
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut std::task::Context<'_>) -> Poll<Result<(), Self::Error>> {
        // The permit is acquired and immediately dropped, as tower does not yet allow returning it.
        // So this is only indicative of capacity being available.
        Pin::new(&mut self.concurrency_limiter.acquire().boxed())
            .poll(cx)
            .map_ok(|_| ())
            .map_err(|_| unreachable!("semaphore is never closed"))
    }

    fn call(&mut self, req: CidFile) -> Self::Future {
        let this = self.cheap_clone();
        async move {
            let _permit = this.concurrency_limiter.acquire().await;
            this.call(&req)
                .await
                .map(|x| (req.clone(), x))
                .map_err(|e| (req.clone(), e))
        }
        .boxed()
    }
}

// Multihashes that are collision resistant. This is not complete but covers the commonly used ones.
// Code table: https://github.com/multiformats/multicodec/blob/master/table.csv
// rust-multihash code enum: https://github.com/multiformats/rust-multihash/blob/master/src/multihash_impl.rs
const SAFE_MULTIHASHES: [u64; 15] = [
    0x0,    // Identity
    0x12,   // SHA2-256 (32-byte hash size)
    0x13,   // SHA2-512 (64-byte hash size)
    0x17,   // SHA3-224 (28-byte hash size)
    0x16,   // SHA3-256 (32-byte hash size)
    0x15,   // SHA3-384 (48-byte hash size)
    0x14,   // SHA3-512 (64-byte hash size)
    0x1a,   // Keccak-224 (28-byte hash size)
    0x1b,   // Keccak-256 (32-byte hash size)
    0x1c,   // Keccak-384 (48-byte hash size)
    0x1d,   // Keccak-512 (64-byte hash size)
    0xb220, // BLAKE2b-256 (32-byte hash size)
    0xb240, // BLAKE2b-512 (64-byte hash size)
    0xb260, // BLAKE2s-256 (32-byte hash size)
    0x1e,   // BLAKE3-256 (32-byte hash size)
];

#[cfg(test)]
mod test {
    use ipfs::IpfsApi;
    use ipfs_api as ipfs;
    use std::{fs, str::FromStr, time::Duration};

    use cid::Cid;
    use graph::{ipfs_client::IpfsClient, tokio};

    use super::IpfsService;
    use uuid::Uuid;

    #[tokio::test]
    async fn cat_file_in_folder() {
        let path = "./tests/fixtures/ipfs_folder";
        let uid = Uuid::new_v4().to_string();
        fs::write(format!("{}/random.txt", path), &uid).unwrap();

        let cl: ipfs::IpfsClient = ipfs::IpfsClient::default();

        let rsp = cl.add_path(&path).await.unwrap();

        let ipfs_folder = rsp.iter().find(|rsp| rsp.name == "ipfs_folder").unwrap();

        let local = IpfsClient::localhost();
        let cid = Cid::from_str(&ipfs_folder.hash).unwrap();
        let file = "random.txt".to_string();

        let svc = IpfsService::new(local, 100000, Duration::from_secs(5), 10);

        let content = svc
            .call(&super::CidFile {
                cid,
                path: Some(file),
            })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(content.to_vec(), uid.as_bytes().to_vec());
    }
}
