use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use slog::{warn, Logger};
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};

/// HostCount is the underlying structure to keep the count,
/// we require that all the hosts are known ahead of time, this way we can
/// avoid locking since we don't need to modify the entire struture.
type HostCount = Arc<HashMap<Host, AtomicU64>>;

#[derive(Debug, Eq, PartialEq, Hash, Clone)]
pub struct Host(Box<str>);

impl From<String> for Host {
    fn from(s: String) -> Self {
        Host(s.into_boxed_str())
    }
}

impl From<&str> for Host {
    fn from(s: &str) -> Self {
        Host(s.into())
    }
}

impl std::ops::Deref for Host {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

enum EndpointMetric {
    Success(Host),
    Failure(Host),
}

#[derive(Debug)]
pub struct EndpointMetrics {
    logger: Logger,
    sender: UnboundedSender<EndpointMetric>,
    hosts: HostCount,
}

impl EndpointMetrics {
    #[cfg(debug_assertions)]
    pub fn noop() -> Self {
        use slog::{o, Discard};
        let (sender, _) = mpsc::unbounded_channel();

        Self {
            logger: Logger::root(Discard, o!()),
            sender,
            hosts: Arc::new(HashMap::default()),
        }
    }

    pub fn success(&self, host: Host) -> anyhow::Result<()> {
        if let Err(e) = self.sender.send(EndpointMetric::Success(host)) {
            warn!(self.logger, "metrics channel has been closed: {}", e)
        }

        Ok(())
    }

    pub fn failure(&self, host: Host) -> anyhow::Result<()> {
        if let Err(e) = self.sender.send(EndpointMetric::Failure(host)) {
            warn!(self.logger, "metrics channel has been closed: {}", e)
        }

        Ok(())
    }

    /// Returns the current error count of a host or 0 if the host
    /// doesn't have a value on the map.
    pub fn get_count(&self, host: &Host) -> u64 {
        self.hosts
            .get(host)
            .map(|c| c.load(Ordering::Relaxed))
            .unwrap_or(0)
    }
}

/// EndpointMetricsProcessor handles metrics by consuming messages from the channel
/// and maintaining the count field through interior mutability.
/// Each time a success call is made through an adapter, it will reset the error count.
/// This processor uses an async design so that the potentially hot path is as quick as
/// possible.
pub struct EndpointMetricsProcessor {
    logger: Logger,
    // This is an Arc because we want the reader to have a pointer for it. The
    // the hashmap itself won't be modified
    hosts: HostCount,
    receiver: UnboundedReceiver<EndpointMetric>,
}

impl EndpointMetricsProcessor {
    /// This is a convenience function for launching the background task automatically
    /// Each pair will track different metrics so for production use this should be
    /// shared across all client.
    pub fn tokio_spawn(logger: Logger, hosts: &[String]) -> EndpointMetrics {
        let (processor, metrics) = Self::new(logger, hosts);
        tokio::spawn(processor.run());
        metrics
    }

    pub fn new(logger: Logger, hosts: &[impl AsRef<str>]) -> (Self, EndpointMetrics) {
        let (sender, receiver) = mpsc::unbounded_channel();
        let hosts = Arc::new(HashMap::from_iter(
            hosts
                .iter()
                .map(|h| (Host::from(h.as_ref()), AtomicU64::new(0))),
        ));

        (
            Self {
                logger: logger.clone(),
                hosts: hosts.clone(),
                receiver,
            },
            EndpointMetrics {
                logger,
                sender,
                hosts,
            },
        )
    }

    pub async fn run(mut self) {
        loop {
            match self.receiver.recv().await {
                Some(EndpointMetric::Success(host)) => {
                    if let Some(count) = self.hosts.get(&host) {
                        count.store(0, Ordering::Relaxed);
                    }
                }
                Some(EndpointMetric::Failure(host)) => {
                    if let Some(count) = self.hosts.get(&host) {
                        count.fetch_add(1, Ordering::Relaxed);
                    }
                }
                None => {
                    warn!(self.logger, "endpoint metrics disabled");
                    return;
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::{mem, sync::atomic::Ordering, time::Duration};

    use slog::{o, Discard, Logger};

    use crate::endpoint::Host;

    use super::EndpointMetricsProcessor;

    #[tokio::test]
    async fn should_increment_and_reset() {
        let (a, b, c): (Host, Host, Host) = ("a".into(), "b".into(), "c".into());
        let hosts: &[&str] = &[&a, &b, &c];
        let logger = Logger::root(Discard, o!());

        let (processor, metrics) = EndpointMetricsProcessor::new(logger, hosts);

        metrics.success(a.clone()).unwrap();
        metrics.failure(a.clone()).unwrap();
        metrics.failure(b.clone()).unwrap();
        metrics.failure(b.clone()).unwrap();
        metrics.success(c.clone()).unwrap();
        assert_eq!(metrics.get_count(&a), 0);
        assert_eq!(metrics.get_count(&b), 0);
        assert_eq!(metrics.get_count(&c), 0);

        let hosts = metrics.hosts.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            mem::drop(metrics);
        });

        processor.run().await;

        assert_eq!(hosts.get(&a).unwrap().load(Ordering::Relaxed), 1);
        assert_eq!(hosts.get(&b).unwrap().load(Ordering::Relaxed), 2);
        assert_eq!(hosts.get(&c).unwrap().load(Ordering::Relaxed), 0);
    }
}
