use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use async_trait::async_trait;
use bytes::Bytes;
use futures03::stream::BoxStream;
use slog::info;
use slog::Logger;

use crate::util::security::SafeDisplay;

mod content_path;
mod error;
mod gateway_client;
mod pool;
mod retry_policy;
mod rpc_client;
mod server_address;

pub mod test_utils;

pub use self::content_path::ContentPath;
pub use self::error::IpfsError;
pub use self::error::RequestError;
pub use self::gateway_client::IpfsGatewayClient;
pub use self::rpc_client::IpfsRpcClient;
pub use self::server_address::ServerAddress;

pub type IpfsResult<T> = Result<T, IpfsError>;

/// Describes a read-only connection to an IPFS server.
pub trait IpfsClient: CanProvide + CatStream + Cat + GetBlock + Send + Sync + 'static {}

#[async_trait]
/// Checks if the server can provide data from the specified content path.
pub trait CanProvide {
    /// Checks if the server can provide data from the specified content path.
    async fn can_provide(&self, path: &ContentPath, timeout: Option<Duration>) -> IpfsResult<bool>;
}

#[async_trait]
/// Streams data from the specified content path.
pub trait CatStream {
    /// Streams data from the specified content path.
    async fn cat_stream(
        &self,
        path: &ContentPath,
        timeout: Option<Duration>,
    ) -> IpfsResult<BoxStream<'static, IpfsResult<Bytes>>>;
}

#[async_trait]
/// Downloads data from the specified content path.
pub trait Cat {
    /// Downloads data from the specified content path.
    async fn cat(
        &self,
        path: &ContentPath,
        max_size: usize,
        timeout: Option<Duration>,
    ) -> IpfsResult<Bytes>;
}

#[async_trait]
/// Downloads an IPFS block in raw format.
pub trait GetBlock {
    /// Downloads an IPFS block in raw format.
    async fn get_block(&self, path: &ContentPath, timeout: Option<Duration>) -> IpfsResult<Bytes>;
}

impl<T> IpfsClient for T where T: CanProvide + CatStream + Cat + GetBlock + Send + Sync + 'static {}

/// Creates and returns the most appropriate IPFS client for the given IPFS server addresses.
///
/// If multiple IPFS server addresses are specified, an IPFS client pool is created internally
/// and for each IPFS read request, the fastest client that can provide the content is
/// automatically selected and the request is forwarded to that client.
pub async fn new_ipfs_client<I, S>(
    server_addresses: I,
    logger: &Logger,
) -> IpfsResult<Arc<dyn IpfsClient>>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut clients = Vec::new();

    for server_address in server_addresses {
        let server_address = server_address.as_ref();

        info!(
            logger,
            "Connecting to IPFS server at '{}'",
            SafeDisplay(server_address)
        );

        match IpfsGatewayClient::new(server_address, logger).await {
            Ok(client) => {
                info!(
                    logger,
                    "Successfully connected to IPFS gateway at: '{}'",
                    SafeDisplay(server_address)
                );

                clients.push(client.into_boxed());
                continue;
            }
            Err(err) if err.is_invalid_server() => {}
            Err(err) => return Err(err),
        };

        match IpfsRpcClient::new(server_address, logger).await {
            Ok(client) => {
                info!(
                    logger,
                    "Successfully connected to IPFS RPC API at: '{}'",
                    SafeDisplay(server_address)
                );

                clients.push(client.into_boxed());
                continue;
            }
            Err(err) if err.is_invalid_server() => {}
            Err(err) => return Err(err),
        };

        return Err(IpfsError::InvalidServer {
            server_address: server_address.parse()?,
            reason: anyhow!("unknown server kind"),
        });
    }

    match clients.len() {
        0 => Err(IpfsError::InvalidServerAddress {
            input: "".to_owned(),
            source: anyhow!("at least one server address is required"),
        }),
        1 => Ok(clients.pop().unwrap().into()),
        n => {
            info!(logger, "Creating a pool of {} IPFS clients", n);

            let pool = pool::IpfsClientPool::with_clients(clients);

            Ok(pool.into_boxed().into())
        }
    }
}
