use lazy_static::lazy_static;

use std::{collections::HashMap, sync::Arc, time::Duration};

use tokio::sync::{mpsc, watch::Receiver, RwLock};

use super::store::DeploymentId;

const DEFAULT_BUFFER_SIZE: usize = 100;
#[cfg(not(test))]
const INDEXER_WATCHER_INTERVAL: Duration = Duration::from_secs(10);
#[cfg(test)]
const INDEXER_WATCHER_INTERVAL: Duration = Duration::from_millis(100);
lazy_static! {
    pub static ref TRACING_RUNTIME: tokio::runtime::Runtime =
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
}

#[derive(Debug, Clone)]
pub struct Subscriptions<T> {
    inner: Arc<RwLock<HashMap<DeploymentId, mpsc::Sender<T>>>>,
}

impl<T> Default for Subscriptions<T> {
    fn default() -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

pub type SubscriptionsWatcher<T> = Receiver<Subscriptions<T>>;

/// A control structure for managing tracing subscriptions.
#[derive(Debug)]
pub struct TracingControl<T> {
    watcher: Receiver<HashMap<DeploymentId, mpsc::Sender<T>>>,
    subscriptions: Subscriptions<T>,
    default_buffer_size: usize,
}

impl<T: Send + Clone + 'static> TracingControl<T> {
    /// Starts a new tracing control instance.If an async runtime is not available, a new one will be created.
    pub fn start() -> Self {
        Self::new(DEFAULT_BUFFER_SIZE)
    }

    pub fn new(buffer_size: usize) -> Self {
        let subscriptions = Subscriptions::default();
        let subs = subscriptions.clone();

        let watcher = std::thread::spawn(move || {
            let handle =
                tokio::runtime::Handle::try_current().unwrap_or(TRACING_RUNTIME.handle().clone());

            handle.block_on(async move {
                indexer_watcher::new_watcher(INDEXER_WATCHER_INTERVAL, move || {
                    let subs = subs.clone();

                    async move { Ok(subs.inner.read().await.clone()) }
                })
                .await
            })
        })
        .join()
        .unwrap()
        .unwrap();

        Self {
            watcher,
            subscriptions,
            default_buffer_size: buffer_size,
        }
    }

    /// Returns a producer for a given deployment ID. If the producer is closed, it will return None.
    /// The producer could still be closed in the meantime.
    pub fn producer(&self, key: DeploymentId) -> Option<mpsc::Sender<T>> {
        self.watcher
            .borrow()
            .get(&key)
            .cloned()
            .filter(|sender| !sender.is_closed())
    }

    /// Creates a new subscription for a given deployment ID with a given buffer size. If a subscription already
    /// exists, it will be replaced.
    pub async fn subscribe_with_chan_size(
        &self,
        key: DeploymentId,
        buffer_size: usize,
    ) -> mpsc::Receiver<T> {
        let (tx, rx) = mpsc::channel(buffer_size);
        let mut guard = self.subscriptions.inner.write().await;
        guard.insert(key, tx);

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

    use anyhow::anyhow;
    use tokio_retry::Retry;

    use super::*;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_tracing_control() {
        let control: TracingControl<()> = TracingControl::start();
        let control = Arc::new(control);

        // produce before subscription
        let tx = control.producer(DeploymentId(123));
        assert!(tx.is_none());

        // drop the subscription
        let rx = control.subscribe(DeploymentId(123)).await;

        let c = control.clone();
        // check subscription is none because channel is closed
        let tx = Retry::spawn(vec![INDEXER_WATCHER_INTERVAL; 2].into_iter(), move || {
            let control = c.clone();
            async move {
                match control.producer(DeploymentId(123)) {
                    Some(sender) if !sender.is_closed() => Ok(sender),
                    // producer should never return a closed sender
                    Some(_) => unreachable!(),
                    None => Err(anyhow!("Sender not created yet")),
                }
            }
        })
        .await
        .unwrap();
        assert!(!tx.is_closed());
        drop(rx);

        // check subscription is none because channel is closed
        let tx = control.producer(DeploymentId(123));
        assert!(tx.is_none());

        // re-create subscription
        let _rx = control.subscribe(DeploymentId(123)).await;

        // check old subscription was replaced
        let c = control.clone();
        let tx = Retry::spawn(vec![INDEXER_WATCHER_INTERVAL; 2].into_iter(), move || {
            let tx = c.producer(DeploymentId(123));
            async move {
                match tx {
                    Some(sender) if !sender.is_closed() => Ok(sender),
                    Some(_) => Err(anyhow!("Sender is closed")),
                    None => Err(anyhow!("Sender not created yet")),
                }
            }
        })
        .await
        .unwrap();
        assert!(!tx.is_closed())
    }
}
