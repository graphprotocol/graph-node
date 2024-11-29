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

    /// Does not consider HTTP status codes for timeouts.
    #[error("IPFS request to '{path}' timed out")]
    RequestTimeout { path: ContentPath },

    #[error("IPFS request to '{path}' failed with a deterministic error: {reason:#}")]
    DeterministicFailure {
        path: ContentPath,
        reason: DeterministicIpfsError,
    },

    #[error(transparent)]
    RequestFailed(RequestError),
}

#[derive(Debug, Error)]
pub enum DeterministicIpfsError {}

#[derive(Debug, Error)]
#[error("request to IPFS server failed: {0:#}")]
pub struct RequestError(reqwest::Error);

impl IpfsError {
    /// Returns true if the sever is invalid.
    pub fn is_invalid_server(&self) -> bool {
        matches!(self, Self::InvalidServer { .. })
    }

    /// Returns true if the error was caused by a timeout.
    ///
    /// Considers HTTP status codes for timeouts.
    pub fn is_timeout(&self) -> bool {
        match self {
            Self::RequestTimeout { .. } => true,
            Self::RequestFailed(err) if err.is_timeout() => true,
            _ => false,
        }
    }

    /// Returns true if the error was caused by a network connection failure.
    pub fn is_networking(&self) -> bool {
        matches!(self, Self::RequestFailed(err) if err.is_networking())
    }

    /// Returns true if the error is deterministic.
    pub fn is_deterministic(&self) -> bool {
        match self {
            Self::InvalidServerAddress { .. } => true,
            Self::InvalidServer { .. } => true,
            Self::InvalidContentPath { .. } => true,
            Self::ContentNotAvailable { .. } => false,
            Self::ContentTooLarge { .. } => true,
            Self::RequestTimeout { .. } => false,
            Self::DeterministicFailure { .. } => true,
            Self::RequestFailed(_) => false,
        }
    }
}

impl From<reqwest::Error> for IpfsError {
    fn from(err: reqwest::Error) -> Self {
        // We remove the URL from the error as it may contain
        // sensitive information such as auth tokens or passwords.
        Self::RequestFailed(RequestError(err.without_url()))
    }
}

impl RequestError {
    /// Returns true if the request failed due to a networking error.
    pub fn is_networking(&self) -> bool {
        self.0.is_request() || self.0.is_connect() || self.0.is_timeout()
    }

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
}
