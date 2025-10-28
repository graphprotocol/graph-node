use graph::nozzle::error::IsDeterministic;
use thiserror::Error;

#[derive(Debug, Error)]
pub(super) enum Error {
    #[error("runner failed with a non-deterministic error: {0:#}")]
    NonDeterministic(#[source] anyhow::Error),

    #[error("runner failed with a deterministic error: {0:#}")]
    Deterministic(#[source] anyhow::Error),
}

impl Error {
    pub(super) fn context<C>(self, context: C) -> Self
    where
        C: std::fmt::Display + Send + Sync + 'static,
    {
        match self {
            Self::NonDeterministic(e) => Self::NonDeterministic(e.context(context)),
            Self::Deterministic(e) => Self::Deterministic(e.context(context)),
        }
    }

    pub(super) fn is_deterministic(&self) -> bool {
        match self {
            Self::Deterministic(_) => true,
            Self::NonDeterministic(_) => false,
        }
    }
}

impl<T> From<T> for Error
where
    T: std::error::Error + IsDeterministic + Send + Sync + 'static,
{
    fn from(e: T) -> Self {
        if e.is_deterministic() {
            Self::Deterministic(e.into())
        } else {
            Self::NonDeterministic(e.into())
        }
    }
}
