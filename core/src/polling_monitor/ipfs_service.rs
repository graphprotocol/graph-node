use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Error};
use bytes::Bytes;
use graph::futures03::future::BoxFuture;
use graph::ipfs::ContentPath;
use graph::ipfs::IpfsClient;
use graph::ipfs::IpfsError;
use graph::{derive::CheapClone, prelude::CheapClone};
use tower::{buffer::Buffer, ServiceBuilder, ServiceExt};

pub type IpfsService = Buffer<ContentPath, BoxFuture<'static, Result<Option<Bytes>, Error>>>;

pub fn ipfs_service(
    client: Arc<dyn IpfsClient>,
    max_file_size: usize,
    timeout: Duration,
    rate_limit: u16,
) -> IpfsService {
    let ipfs = IpfsServiceInner {
        client,
        timeout,
        max_file_size,
    };

    let svc = ServiceBuilder::new()
        .rate_limit(rate_limit.into(), Duration::from_secs(1))
        .service_fn(move |req| ipfs.cheap_clone().call_inner(req))
        .boxed();

    // The `Buffer` makes it so the rate limit is shared among clones.
    // Make it unbounded to avoid any risk of starvation.
    Buffer::new(svc, u32::MAX as usize)
}

#[derive(Clone, CheapClone)]
struct IpfsServiceInner {
    client: Arc<dyn IpfsClient>,
    timeout: Duration,
    max_file_size: usize,
}

impl IpfsServiceInner {
    async fn call_inner(self, path: ContentPath) -> Result<Option<Bytes>, Error> {
        let multihash = path.cid().hash().code();
        if !SAFE_MULTIHASHES.contains(&multihash) {
            return Err(anyhow!("CID multihash {} is not allowed", multihash));
        }

        let res = self
            .client
            .cat(&path, self.max_file_size, Some(self.timeout))
            .await;

        match res {
            Ok(file_bytes) => Ok(Some(file_bytes)),
            Err(IpfsError::RequestFailed(err)) if err.is_timeout() => {
                // Timeouts in IPFS mean that the content is not available, so we return `None`.
                Ok(None)
            }
            Err(err) => Err(err.into()),
        }
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
    use std::time::Duration;

    use graph::components::link_resolver::ArweaveClient;
    use graph::components::link_resolver::ArweaveResolver;
    use graph::data::value::Word;
    use graph::ipfs::test_utils::add_files_to_local_ipfs_node_for_testing;
    use graph::ipfs::IpfsRpcClient;
    use graph::ipfs::ServerAddress;
    use graph::tokio;
    use tower::ServiceExt;
    use uuid::Uuid;

    use super::*;

    #[tokio::test]
    async fn cat_file_in_folder() {
        let random_bytes = Uuid::new_v4().as_bytes().to_vec();
        let ipfs_file = ("dir/file.txt", random_bytes.clone());

        let add_resp = add_files_to_local_ipfs_node_for_testing([ipfs_file])
            .await
            .unwrap();

        let dir_cid = add_resp.into_iter().find(|x| x.name == "dir").unwrap().hash;

        let client =
            IpfsRpcClient::new_unchecked(ServerAddress::local_rpc_api(), &graph::log::discard())
                .unwrap()
                .into_boxed();

        let svc = ipfs_service(client.into(), 100000, Duration::from_secs(30), 10);

        let path = ContentPath::new(format!("{dir_cid}/file.txt")).unwrap();
        let content = svc.oneshot(path).await.unwrap().unwrap();

        assert_eq!(content.to_vec(), random_bytes);
    }

    #[tokio::test]
    async fn arweave_get() {
        const ID: &str = "8APeQ5lW0-csTcBaGdPBDLAL2ci2AT9pTn2tppGPU_8";

        let cl = ArweaveClient::default();
        let body = cl.get(&Word::from(ID)).await.unwrap();
        let body = String::from_utf8(body).unwrap();

        let expected = r#"
            {"name":"Arloader NFT #1","description":"Super dope, one of a kind NFT","collection":{"name":"Arloader NFT","family":"We AR"},"attributes":[{"trait_type":"cx","value":-0.4042198883730073},{"trait_type":"cy","value":0.5641681708263335},{"trait_type":"iters","value":44}],"properties":{"category":"image","files":[{"uri":"https://arweave.net/7gWCr96zc0QQCXOsn5Vk9ROVGFbMaA9-cYpzZI8ZMDs","type":"image/png"},{"uri":"https://arweave.net/URwQtoqrbYlc5183STNy3ZPwSCRY4o8goaF7MJay3xY/1.png","type":"image/png"}]},"image":"https://arweave.net/URwQtoqrbYlc5183STNy3ZPwSCRY4o8goaF7MJay3xY/1.png"}
        "#.trim_start().trim_end();
        assert_eq!(expected, body);
    }
}
