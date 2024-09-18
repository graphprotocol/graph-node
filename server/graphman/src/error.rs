use thiserror::Error;

#[derive(Debug, Error)]
pub enum GraphmanServerError {
    #[error("invalid auth token: {0:#}")]
    InvalidAuthToken(#[source] anyhow::Error),

    #[error("I/O error: {0:#}")]
    Io(#[source] anyhow::Error),
}
