use std::sync::Arc;

use anyhow::anyhow;
use slog::info;
use slog::Logger;

use crate::util::security::SafeDisplay;

mod client;
mod content_path;
mod error;
mod gateway_client;
mod pool;
mod retry_policy;
mod rpc_client;
mod server_address;

pub mod test_utils;

pub use self::client::IpfsClient;
pub use self::client::IpfsRequest;
pub use self::client::IpfsResponse;
pub use self::content_path::ContentPath;
pub use self::error::IpfsError;
pub use self::error::RequestError;
pub use self::gateway_client::IpfsGatewayClient;
pub use self::pool::IpfsClientPool;
pub use self::retry_policy::RetryPolicy;
pub use self::rpc_client::IpfsRpcClient;
pub use self::server_address::ServerAddress;

pub type IpfsResult<T> = Result<T, IpfsError>;

/// Creates and returns the most appropriate IPFS client for the given IPFS server addresses.
///
/// If multiple IPFS server addresses are specified, an IPFS client pool is created internally
/// and for each IPFS request, the fastest client that can provide the content is
/// automatically selected and the response is streamed from that client.
pub async fn new_ipfs_client<I, S>(
    server_addresses: I,
    logger: &Logger,
) -> IpfsResult<Arc<dyn IpfsClient>>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut clients: Vec<Arc<dyn IpfsClient>> = Vec::new();

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

                clients.push(Arc::new(client));
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

                clients.push(Arc::new(client));
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

            let pool = IpfsClientPool::new(clients, logger);

            Ok(Arc::new(pool))
        }
    }
}
