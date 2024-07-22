use thiserror::Error;

#[derive(Debug, Error)]
pub enum GraphmanError {
    #[error("datastore error: {0}")]
    Datastore(anyhow::Error),
}

impl From<graph::components::store::StoreError> for GraphmanError {
    fn from(err: graph::components::store::StoreError) -> Self {
        Self::Datastore(err.into())
    }
}

impl From<diesel::result::Error> for GraphmanError {
    fn from(err: diesel::result::Error) -> Self {
        Self::Datastore(err.into())
    }
}
