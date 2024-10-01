use thiserror::Error;

#[derive(Debug, Error)]
pub enum GraphmanError {
    #[error("store error: {0:#}")]
    Store(#[source] anyhow::Error),
}

impl From<graph::components::store::StoreError> for GraphmanError {
    fn from(err: graph::components::store::StoreError) -> Self {
        Self::Store(err.into())
    }
}

impl From<diesel::result::Error> for GraphmanError {
    fn from(err: diesel::result::Error) -> Self {
        Self::Store(err.into())
    }
}
