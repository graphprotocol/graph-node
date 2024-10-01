use async_graphql::SimpleObject;

/// This type is used when an operation has been successful,
/// but there is no output that can be returned.
#[derive(Clone, Debug, SimpleObject)]
pub struct EmptyResponse {
    pub success: bool,
}

impl EmptyResponse {
    /// Returns a successful response.
    pub fn new() -> Self {
        Self { success: true }
    }
}
