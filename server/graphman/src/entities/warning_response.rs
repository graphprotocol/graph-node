use async_graphql::SimpleObject;

#[derive(Clone, Debug, SimpleObject)]
pub struct CompletedWithWarnings {
    pub warnings: Vec<String>,
}

impl CompletedWithWarnings {
    /// Returns a response with success & message.
    pub fn new(msg: Vec<String>) -> Self {
        Self { warnings: msg }
    }
}
