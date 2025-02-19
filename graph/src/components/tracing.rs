use std::{collections::HashMap, sync::Arc, time::Duration};

use futures03::TryFutureExt;
use slog::{o, Logger};
use tokio::sync::{mpsc, watch::Receiver, RwLock};

use crate::prelude::LoggerFactory;

use super::store::DeploymentId;

const DEFAULT_BUFFER_SIZE: usize = 100;

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

// impl<T: Send + Clone + 'static> Default for TracingControl<T> {
// fn default() -> Self {
//     let subscriptions = Subscriptions::default();
//     let subs = subscriptions.clone();
//     let watcher = std::thread::spawn(move || {
//         let runtime = tokio::runtime::Builder::new_multi_thread()
//             .enable_all()
//             .build()
//             .unwrap();
//         runtime.block_on()
//     })
//     .join()
//     .unwrap()
//     .unwrap();

//     Self {
//         subscriptions,
//         default_buffer_size: DEFAULT_BUFFER_SIZE,
//         watcher,
//     }
// }
// }

impl<T: Send + Clone + 'static> TracingControl<T> {
    pub async fn start() -> Self {
        let subscriptions = Subscriptions::default();
        let subs = subscriptions.clone();
        let watcher = indexer_watcher::new_watcher(
            #[cfg(test)]
            Duration::from_millis(100),
            #[cfg(not(test))]
            Duration::from_secs(30),
            move || {
                let subs = subs.clone();

                async move { Ok(subs.inner.read().await.clone()) }
            },
        )
        .await
        .unwrap();
        Self {
            watcher,
            subscriptions,
            default_buffer_size: DEFAULT_BUFFER_SIZE,
        }
    }
    // pub fn new(default_buffer_size: Option<usize>) -> Self {
    //     Self {
    //         default_buffer_size: default_buffer_size.unwrap_or(DEFAULT_BUFFER_SIZE),
    //         ..Default::default()
    //     }
    // }

    pub fn producer(&self, key: DeploymentId) -> Option<mpsc::Sender<T>> {
        self.watcher
            .borrow()
            .get(&key)
            .cloned()
            .filter(|sender| !sender.is_closed())
    }

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
    use std::{future::IntoFuture, sync::Arc};

    #[tokio::test]
    async fn test_tracing_control() {
        let control: TracingControl<()> = TracingControl::start().await;
        let control = Arc::new(control);

        // produce before subscription
        let tx = control.producer(DeploymentId(123));
        assert!(tx.is_none());

        // drop the subscription
        let rx = control.subscribe(DeploymentId(123));

        let c = control.clone();
        // check subscription is none because channel is closed
        let tx = Retry::spawn(vec![Duration::from_secs(5); 10].into_iter(), move || {
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
        let _rx = control.subscribe(DeploymentId(123));
        // check old subscription was replaced
        let tx = control.producer(DeploymentId(123));
        assert!(!tx.unwrap().is_closed())
    }
}
