use std::fmt::Display;
use std::hash::Hash;

use graph::{data_source::offchain::Base64, ipfs::ContentPath};

use crate::polling_monitor::ipfs_service::IpfsRequest;

/// Request ID is used to create backoffs on request failures.
pub trait RequestId {
    type Id: Clone + Display + Eq + Hash + Send + Sync + 'static;

    /// Returns the ID of the request.
    fn request_id(&self) -> &Self::Id;
}

impl RequestId for IpfsRequest {
    type Id = ContentPath;

    fn request_id(&self) -> &ContentPath {
        &self.path
    }
}

impl RequestId for Base64 {
    type Id = Base64;

    fn request_id(&self) -> &Base64 {
        self
    }
}

#[cfg(debug_assertions)]
impl RequestId for &'static str {
    type Id = &'static str;

    fn request_id(&self) -> &Self::Id {
        self
    }
}
