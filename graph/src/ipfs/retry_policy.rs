use slog::Logger;

use crate::ipfs::error::IpfsError;
use crate::prelude::*;
use crate::util::futures::retry;
use crate::util::futures::RetryConfig;

/// Describes retry behavior when IPFS requests fail.
#[derive(Clone, Copy, Debug)]
pub enum RetryPolicy {
    /// At the first error, immediately stops execution and returns the error.
    None,

    /// Retries the request if the error is related to the network connection.
    Networking,

    /// Retries the request if the error is related to the network connection,
    /// and for any error that may be resolved by sending another request.
    NonDeterministic,
}

impl RetryPolicy {
    /// Creates a retry policy for every request sent to IPFS servers.
    pub(super) fn create<O: Send + Sync + 'static>(
        self,
        operation_name: impl ToString,
        logger: &Logger,
    ) -> RetryConfig<O, IpfsError> {
        retry(operation_name, logger)
            .limit(ENV_VARS.mappings.ipfs_max_attempts)
            .max_delay(ENV_VARS.ipfs_request_timeout)
            .when(move |result: &Result<O, IpfsError>| match result {
                Ok(_) => false,
                Err(err) => match self {
                    Self::None => false,
                    Self::Networking => err.is_networking(),
                    Self::NonDeterministic => !err.is_deterministic(),
                },
            })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;
    use crate::ipfs::ContentPath;
    use crate::log::discard;

    const CID: &str = "QmUNLLsPACCz1vLxQVkXqqLX5R1X345qqfHbsf67hvA3Nn";

    fn path() -> ContentPath {
        ContentPath::new(CID).unwrap()
    }

    #[crate::test]
    async fn retry_policy_none_disables_retries() {
        let counter = Arc::new(AtomicU64::new(0));

        let err = RetryPolicy::None
            .create::<()>("test", &discard())
            .no_timeout()
            .run({
                let counter = counter.clone();
                move || {
                    let counter = counter.clone();
                    async move {
                        counter.fetch_add(1, Ordering::SeqCst);
                        Err(IpfsError::RequestTimeout { path: path() })
                    }
                }
            })
            .await
            .unwrap_err();

        assert_eq!(counter.load(Ordering::SeqCst), 1);
        assert!(matches!(err, IpfsError::RequestTimeout { .. }));
    }

    #[crate::test]
    async fn retry_policy_networking_retries_only_network_related_errors() {
        let counter = Arc::new(AtomicU64::new(0));

        let err = RetryPolicy::Networking
            .create("test", &discard())
            .no_timeout()
            .run({
                let counter = counter.clone();
                move || {
                    let counter = counter.clone();
                    async move {
                        counter.fetch_add(1, Ordering::SeqCst);

                        if counter.load(Ordering::SeqCst) == 10 {
                            return Err(IpfsError::RequestTimeout { path: path() });
                        }

                        reqwest::Client::new()
                            .get("https://simulate-dns-lookup-failure")
                            .timeout(Duration::from_millis(50))
                            .send()
                            .await?;

                        Ok(())
                    }
                }
            })
            .await
            .unwrap_err();

        assert_eq!(counter.load(Ordering::SeqCst), 10);
        assert!(matches!(err, IpfsError::RequestTimeout { .. }));
    }

    #[crate::test]
    async fn retry_policy_networking_stops_on_success() {
        let counter = Arc::new(AtomicU64::new(0));

        RetryPolicy::Networking
            .create("test", &discard())
            .no_timeout()
            .run({
                let counter = counter.clone();
                move || {
                    let counter = counter.clone();
                    async move {
                        counter.fetch_add(1, Ordering::SeqCst);

                        if counter.load(Ordering::SeqCst) == 10 {
                            return Ok(());
                        }

                        reqwest::Client::new()
                            .get("https://simulate-dns-lookup-failure")
                            .timeout(Duration::from_millis(50))
                            .send()
                            .await?;

                        Ok(())
                    }
                }
            })
            .await
            .unwrap();

        assert_eq!(counter.load(Ordering::SeqCst), 10);
    }

    #[crate::test]
    async fn retry_policy_non_deterministic_retries_all_non_deterministic_errors() {
        let counter = Arc::new(AtomicU64::new(0));

        let err = RetryPolicy::NonDeterministic
            .create::<()>("test", &discard())
            .no_timeout()
            .run({
                let counter = counter.clone();
                move || {
                    let counter = counter.clone();
                    async move {
                        counter.fetch_add(1, Ordering::SeqCst);

                        if counter.load(Ordering::SeqCst) == 10 {
                            return Err(IpfsError::ContentTooLarge {
                                path: path(),
                                max_size: 0,
                            });
                        }

                        Err(IpfsError::RequestTimeout { path: path() })
                    }
                }
            })
            .await
            .unwrap_err();

        assert_eq!(counter.load(Ordering::SeqCst), 10);
        assert!(matches!(err, IpfsError::ContentTooLarge { .. }));
    }

    #[crate::test]
    async fn retry_policy_non_deterministic_stops_on_success() {
        let counter = Arc::new(AtomicU64::new(0));

        RetryPolicy::NonDeterministic
            .create("test", &discard())
            .no_timeout()
            .run({
                let counter = counter.clone();
                move || {
                    let counter = counter.clone();
                    async move {
                        counter.fetch_add(1, Ordering::SeqCst);

                        if counter.load(Ordering::SeqCst) == 10 {
                            return Ok(());
                        }

                        Err(IpfsError::RequestTimeout { path: path() })
                    }
                }
            })
            .await
            .unwrap();

        assert_eq!(counter.load(Ordering::SeqCst), 10);
    }
}
