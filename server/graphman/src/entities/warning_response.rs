use async_graphql::SimpleObject;

#[derive(Clone, Debug, SimpleObject)]
pub struct CompletedWithWarnings {
    pub warnings: Vec<String>,
}

impl CompletedWithWarnings {
    pub fn new(warnings: Vec<String>) -> Self {
        Self { warnings }
    }
}
