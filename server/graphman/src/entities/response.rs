use async_graphql::SimpleObject;

#[derive(Clone, Debug, SimpleObject)]
pub struct Response {
    pub success: bool,
    pub message: String,
}

impl Response {
    /// Returns a response with success & message.
    pub fn new(success: bool, msg: String) -> Self {
        Self {
            success,
            message: msg,
        }
    }
}
