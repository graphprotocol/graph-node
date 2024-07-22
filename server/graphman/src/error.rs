use thiserror::Error;

#[derive(Debug, Error)]
pub enum GraphmanServerError {
    #[error("invalid auth token: {0}")]
    InvalidAuthToken(anyhow::Error),
}
