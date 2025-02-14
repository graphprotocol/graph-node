use std::{collections::HashMap, sync::atomic::AtomicBool};

use tokio::sync::{mpsc, RwLock};

use super::store::DeploymentId;

const DEFAULT_BUFFER_SIZE: usize = 100;

#[derive(Debug)]
pub struct Subscriptions<T> {
    inner: RwLock<HashMap<DeploymentId, mpsc::Sender<T>>>,
}

/// A control structure for managing tracing subscriptions.
#[derive(Debug)]
pub struct TracingControl<T> {
    enabled: AtomicBool,
    subscriptions: Subscriptions<T>,
    default_buffer_size: usize,
}

impl<T> Default for TracingControl<T> {
    fn default() -> Self {
        Self {
            enabled: AtomicBool::new(false),
            subscriptions: Subscriptions {
                inner: RwLock::new(HashMap::new()),
            },
            default_buffer_size: DEFAULT_BUFFER_SIZE,
        }
    }
}

impl<T> TracingControl<T> {
    pub fn new(default_buffer_size: Option<usize>) -> Self {
        Self {
            enabled: AtomicBool::new(false),
            subscriptions: Subscriptions {
                inner: RwLock::new(HashMap::new()),
            },
            default_buffer_size: default_buffer_size.unwrap_or(DEFAULT_BUFFER_SIZE),
        }
    }

    /// Creates a channel sender for the given deployment ID. Only one subscription
    /// can exist for a given deployment ID. If tracing is disabled or no subscription
    /// exists, it will return None. Calling producer when a dead subscription exists
    /// will incur a cleanup cost.
    pub async fn producer(&self, key: DeploymentId) -> Option<mpsc::Sender<T>> {
        if !self.enabled.load(std::sync::atomic::Ordering::Relaxed) {
            return None;
        }

        let subs = self.subscriptions.inner.read().await;
        let tx = subs.get(&key);

        match tx {
            Some(tx) if tx.is_closed() => {
                drop(subs);
                let mut subs = self.subscriptions.inner.write().await;
                subs.remove(&key);

                if subs.is_empty() {
                    self.enabled
                        .store(false, std::sync::atomic::Ordering::Relaxed);
                }

                None
            }
            None => None,
            tx => tx.cloned(),
        }
    }
    pub async fn subscribe_with_chan_size(
        &self,
        key: DeploymentId,
        buffer_size: usize,
    ) -> mpsc::Receiver<T> {
        let (tx, rx) = mpsc::channel(buffer_size);
        let mut guard = self.subscriptions.inner.write().await;
        guard.insert(key, tx);
        self.enabled
            .store(true, std::sync::atomic::Ordering::Relaxed);

        rx
    }

    /// Creates a new subscription for a given deployment ID. If a subscription already
    /// exists, it will be replaced.
    pub async fn subscribe(&self, key: DeploymentId) -> mpsc::Receiver<T> {
        self.subscribe_with_chan_size(key, self.default_buffer_size)
            .await
    }
}

#[cfg(test)]
mod test {

    use super::*;
    use std::sync::atomic::Ordering::Relaxed;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_tracing_control() {
        let control: TracingControl<()> = TracingControl::default();
        let control = Arc::new(control);
        assert_eq!(false, control.enabled.load(Relaxed));

        let tx = control.producer(DeploymentId(123)).await;
        assert!(tx.is_none());

        let rx = control.subscribe(DeploymentId(123)).await;
        assert_eq!(true, control.enabled.load(Relaxed));

        drop(rx);
        let tx = control.producer(DeploymentId(123)).await;
        assert!(tx.is_none());
        assert_eq!(false, control.enabled.load(Relaxed));

        _ = control.subscribe(DeploymentId(123)).await;
        assert_eq!(true, control.enabled.load(Relaxed));
    }
}
