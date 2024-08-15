use std::sync::Arc;

use anyhow::anyhow;
use http::uri::Scheme;
use http::Uri;

use crate::derive::CheapClone;
use crate::ipfs::IpfsError;
use crate::ipfs::IpfsResult;

#[derive(Clone, Debug, CheapClone)]
/// Contains a valid IPFS server address.
pub struct ServerAddress {
    inner: Arc<str>,
}

impl ServerAddress {
    /// Creates a new [ServerAddress] from the specified input.
    pub fn new(input: impl AsRef<str>) -> IpfsResult<Self> {
        let input = input.as_ref();

        if input.is_empty() {
            return Err(IpfsError::InvalidServerAddress {
                input: input.to_owned(),
                source: anyhow!("address is empty"),
            });
        }

        let uri = input
            .parse::<Uri>()
            .map_err(|err| IpfsError::InvalidServerAddress {
                input: input.to_owned(),
                source: err.into(),
            })?;

        let scheme = uri
            .scheme()
            // Default to HTTP for backward compatibility.
            .unwrap_or(&Scheme::HTTP);

        let authority = uri
            .authority()
            .ok_or_else(|| IpfsError::InvalidServerAddress {
                input: input.to_owned(),
                source: anyhow!("missing authority"),
            })?;

        let mut inner = format!("{scheme}://");

        // In the case of IPFS gateways, depending on the configuration, path requests are
        // sometimes redirected to the subdomain resolver. This is a problem for localhost because
        // some operating systems do not allow subdomain DNS resolutions on localhost for security
        // reasons. To avoid forcing users to always specify an IP address instead of localhost
        // when they want to use a local IPFS gateway, we will naively try to do this for them.
        if authority.host().to_lowercase() == "localhost" {
            inner.push_str("127.0.0.1");

            if let Some(port) = authority.port_u16() {
                inner.push_str(&format!(":{port}"));
            }
        } else {
            inner.push_str(authority.as_str());
        }

        inner.push_str(uri.path().trim_end_matches('/'));
        inner.push('/');

        Ok(Self {
            inner: inner.into(),
        })
    }

    pub fn local_gateway() -> Self {
        Self::new("http://127.0.0.1:8080").unwrap()
    }

    pub fn local_rpc_api() -> Self {
        Self::new("http://127.0.0.1:5001").unwrap()
    }
}

impl std::str::FromStr for ServerAddress {
    type Err = IpfsError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s)
    }
}

impl AsRef<str> for ServerAddress {
    fn as_ref(&self) -> &str {
        &self.inner
    }
}

impl std::fmt::Display for ServerAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fails_on_an_empty_address() {
        let err = ServerAddress::new("").unwrap_err();

        assert_eq!(
            err.to_string(),
            "'' is not a valid IPFS server address: address is empty",
        );
    }

    #[test]
    fn requires_an_authority() {
        let err = ServerAddress::new("https://").unwrap_err();

        assert_eq!(
            err.to_string(),
            "'https://' is not a valid IPFS server address: invalid format",
        );
    }

    #[test]
    fn accepts_a_valid_address() {
        let addr = ServerAddress::new("https://example.com/").unwrap();

        assert_eq!(addr.to_string(), "https://example.com/");
    }

    #[test]
    fn defaults_to_http_scheme() {
        let addr = ServerAddress::new("example.com").unwrap();

        assert_eq!(addr.to_string(), "http://example.com/");
    }

    #[test]
    fn accepts_a_valid_address_with_a_port() {
        let addr = ServerAddress::new("https://example.com:8080/").unwrap();

        assert_eq!(addr.to_string(), "https://example.com:8080/");
    }

    #[test]
    fn rewrites_localhost_to_ipv4() {
        let addr = ServerAddress::new("https://localhost/").unwrap();

        assert_eq!(addr.to_string(), "https://127.0.0.1/");
    }

    #[test]
    fn maintains_the_port_on_localhost_rewrite() {
        let addr = ServerAddress::new("https://localhost:8080/").unwrap();

        assert_eq!(addr.to_string(), "https://127.0.0.1:8080/");
    }

    #[test]
    fn keeps_the_path_in_an_address() {
        let addr = ServerAddress::new("https://example.com/ipfs/").unwrap();

        assert_eq!(addr.to_string(), "https://example.com/ipfs/");
    }

    #[test]
    fn removes_the_query_from_an_address() {
        let addr = ServerAddress::new("https://example.com/?format=json").unwrap();

        assert_eq!(addr.to_string(), "https://example.com/");
    }

    #[test]
    fn adds_a_final_slash() {
        let addr = ServerAddress::new("https://example.com").unwrap();

        assert_eq!(addr.to_string(), "https://example.com/");

        let addr = ServerAddress::new("https://example.com/ipfs").unwrap();

        assert_eq!(addr.to_string(), "https://example.com/ipfs/");
    }

    #[test]
    fn local_gateway_server_address_is_valid() {
        let addr = ServerAddress::local_gateway();

        assert_eq!(addr.to_string(), "http://127.0.0.1:8080/");
    }

    #[test]
    fn local_rpc_api_server_address_is_valid() {
        let addr = ServerAddress::local_rpc_api();

        assert_eq!(addr.to_string(), "http://127.0.0.1:5001/");
    }
}
