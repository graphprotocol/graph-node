mod explorer;
mod request;
mod resolver;
mod schema;
mod server;
mod service;

use lazy_static::lazy_static;

lazy_static! {
    /// A public [`PoiProtection::from_env`] value for easy access.
    pub static ref POI_PROTECTION: PoiProtection = PoiProtection::from_env();
}

/// Validation logic for access tokens required to access POI results.
pub struct PoiProtection {
    access_token: Option<String>,
}

impl PoiProtection {
    /// Creates a new [`PoiProtection`] instance configured in accordance with
    /// the `GRAPH_POI_ACCESS_TOKEN` environment variable.
    pub fn from_env() -> Self {
        let access_token = std::env::var("GRAPH_POI_ACCESS_TOKEN").ok();
        Self { access_token }
    }

    /// Returns `true` iff the given access token allows access to POI results.
    /// When set to [`None`], access will only be permitted when
    /// [`PoiProtection::is_active`] is `false`.
    pub fn validate_access_token(&self, access_token: Option<&str>) -> bool {
        match (self.access_token.as_ref(), access_token) {
            // No active protection.
            (None, _) => true,
            // Protection is active, but no access token was provided.
            (Some(_), None) => false,
            (Some(a), Some(b)) => {
                // When comparing secrets to untrusted user data, we have to be
                // careful about timing attacks. Constant-time comparison is the
                // standard choice in these situations, but it can be quite
                // convoluted. Instead, we'll compare the BLAKE3 hashes of the
                // two values: this way we don't have to worry about timing
                // attacks nor vetting a constant-time comparison crate.
                //
                // We get 128 bits of security out of the box (256/2), which
                // is plenty.
                let hash_a = blake3::hash(a.as_bytes());
                let hash_b = blake3::hash(b.as_bytes());
                hash_a == hash_b
            }
        }
    }

    /// Returns `true` iff POI results protection is configured.
    pub fn is_active(&self) -> bool {
        self.access_token.is_some()
    }
}

pub use self::request::IndexNodeRequest;
pub use self::server::IndexNodeServer;
pub use self::service::{IndexNodeService, IndexNodeServiceResponse};
