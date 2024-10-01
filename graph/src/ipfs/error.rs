use reqwest::StatusCode;
use thiserror::Error;

use crate::ipfs::ContentPath;
use crate::ipfs::ServerAddress;

#[derive(Debug, Error)]
pub enum IpfsError {
    #[error("'{input}' is not a valid IPFS server address: {source:#}")]
    InvalidServerAddress {
        input: String,
        source: anyhow::Error,
    },

    #[error("'{server_address}' is not a valid IPFS server: {reason:#}")]
    InvalidServer {
        server_address: ServerAddress,

        #[source]
        reason: anyhow::Error,
    },

    #[error("'{input}' is not a valid IPFS content path: {source:#}")]
    InvalidContentPath {
        input: String,
        source: anyhow::Error,
    },

    #[error("IPFS content from '{path}' is not available: {reason:#}")]
    ContentNotAvailable {
        path: ContentPath,

        #[source]
        reason: anyhow::Error,
    },

    #[error("IPFS content from '{path}' exceeds the {max_size} bytes limit")]
    ContentTooLarge { path: ContentPath, max_size: usize },

    #[error(transparent)]
    RequestFailed(RequestError),
}

#[derive(Debug, Error)]
#[error("request to IPFS server failed: {0:#}")]
pub struct RequestError(reqwest::Error);

impl IpfsError {
    pub fn is_invalid_server(&self) -> bool {
        matches!(self, Self::InvalidServer { .. })
    }
}

impl From<reqwest::Error> for IpfsError {
    fn from(err: reqwest::Error) -> Self {
        Self::RequestFailed(RequestError(err))
    }
}

impl RequestError {
    /// Returns true if the request failed due to a timeout.
    pub fn is_timeout(&self) -> bool {
        if self.0.is_timeout() {
            return true;
        }

        let Some(status) = self.0.status() else {
            return false;
        };

        const CLOUDFLARE_CONNECTION_TIMEOUT: u16 = 522;
        const CLOUDFLARE_REQUEST_TIMEOUT: u16 = 524;

        [
            StatusCode::REQUEST_TIMEOUT,
            StatusCode::GATEWAY_TIMEOUT,
            StatusCode::from_u16(CLOUDFLARE_CONNECTION_TIMEOUT).unwrap(),
            StatusCode::from_u16(CLOUDFLARE_REQUEST_TIMEOUT).unwrap(),
        ]
        .into_iter()
        .any(|x| status == x)
    }

    /// Returns true if the request can be retried.
    pub fn is_retriable(&self) -> bool {
        let Some(status) = self.0.status() else {
            return true;
        };

        [
            StatusCode::TOO_MANY_REQUESTS,
            StatusCode::INTERNAL_SERVER_ERROR,
            StatusCode::BAD_GATEWAY,
            StatusCode::SERVICE_UNAVAILABLE,
        ]
        .into_iter()
        .any(|x| status == x)
    }
}
