use thiserror::Error;

use crate::amp::error::IsDeterministic;

#[derive(Debug, Error)]
pub enum Error {
    #[error("failed to aggregate record batches: {0:#}")]
    Aggregation(#[source] anyhow::Error),

    #[error("failed to buffer record batches from stream {stream_index}: {source:#}")]
    Buffer {
        stream_index: usize,
        source: anyhow::Error,
    },

    #[error("failed to read record batch from stream {stream_index}: {source:#}")]
    Stream {
        stream_index: usize,
        source: anyhow::Error,
        is_deterministic: bool,
    },
}

impl Error {
    pub(super) fn stream<E>(stream_index: usize, e: E) -> Self
    where
        E: std::error::Error + IsDeterministic + Send + Sync + 'static,
    {
        let is_deterministic = e.is_deterministic();

        Self::Stream {
            stream_index,
            source: anyhow::Error::from(e),
            is_deterministic,
        }
    }
}

impl IsDeterministic for Error {
    fn is_deterministic(&self) -> bool {
        match self {
            Self::Aggregation(_) => true,
            Self::Buffer { .. } => true,
            Self::Stream {
                is_deterministic, ..
            } => *is_deterministic,
        }
    }
}
