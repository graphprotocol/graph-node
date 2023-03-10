use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use slog::{warn, Logger};

use crate::data::value::Word;

/// HostCount is the underlying structure to keep the count,
/// we require that all the hosts are known ahead of time, this way we can
/// avoid locking since we don't need to modify the entire struture.
type HostCount = Arc<HashMap<Host, AtomicU64>>;

/// Host represents the normalized (parse::<Url>().to_string()) of the
/// underlying endpoint. This allows us to track errors across multiple
/// adapters if they share the same endpoint.
pub type Host = Word;

/// EndpointMetrics keeps track of calls success rate for specific calls,
/// a success call to a host will clear the error count.
#[derive(Debug)]
pub struct EndpointMetrics {
    logger: Logger,
    hosts: HostCount,
}

impl EndpointMetrics {
    pub fn new(logger: Logger, hosts: &[impl AsRef<str>]) -> Self {
        let hosts = Arc::new(HashMap::from_iter(
            hosts
                .iter()
                .map(|h| (Host::from(h.as_ref()), AtomicU64::new(0))),
        ));

        Self {
            logger: logger.clone(),
            hosts: hosts.clone(),
        }
    }

    /// This should only be used for testing.
    pub fn mock() -> Self {
        use slog::{o, Discard};
        Self {
            logger: Logger::root(Discard, o!()),
            hosts: Arc::new(HashMap::default()),
        }
    }

    pub fn success(&self, host: &Host) {
        match self.hosts.get(host) {
            Some(count) => {
                count.store(0, Ordering::Relaxed);
            }
            None => warn!(&self.logger, "metrics not available for host {}", host),
        }
    }

    pub fn failure(&self, host: &Host) {
        match self.hosts.get(host) {
            Some(count) => {
                count.fetch_add(1, Ordering::Relaxed);
            }
            None => warn!(&self.logger, "metrics not available for host {}", host),
        }
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

#[cfg(test)]
mod test {
    use slog::{o, Discard, Logger};

    use crate::endpoint::{EndpointMetrics, Host};

    #[tokio::test]
    async fn should_increment_and_reset() {
        let (a, b, c): (Host, Host, Host) = ("a".into(), "b".into(), "c".into());
        let hosts: &[&str] = &[&a, &b, &c];
        let logger = Logger::root(Discard, o!());

        let metrics = EndpointMetrics::new(logger, hosts);

        metrics.success(&a);
        metrics.failure(&a);
        metrics.failure(&b);
        metrics.failure(&b);
        metrics.success(&c);

        assert_eq!(metrics.get_count(&a), 1);
        assert_eq!(metrics.get_count(&b), 2);
        assert_eq!(metrics.get_count(&c), 0);
    }
}
