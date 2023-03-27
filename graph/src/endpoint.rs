use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use prometheus::IntCounterVec;
use slog::{warn, Logger};

use crate::{components::metrics::MetricsRegistry, data::value::Word};

/// HostCount is the underlying structure to keep the count,
/// we require that all the hosts are known ahead of time, this way we can
/// avoid locking since we don't need to modify the entire struture.
type HostCount = Arc<HashMap<Host, AtomicU64>>;

/// Host represents the normalized (parse::<Url>().to_string()) of the
/// underlying endpoint. This allows us to track errors across multiple
/// adapters if they share the same endpoint.
pub type Host = Word;

/// This struct represents all the current labels except for the result
/// which is added separately. If any new labels are necessary they should
/// remain in the same order as added in [`EndpointMetrics::new`]
#[derive(Clone)]
pub struct RequestLabels {
    pub host: Host,
    pub req_type: Word,
    pub conn_type: ConnectionType,
}

/// The type of underlying connection we are reporting for.
#[derive(Clone)]
pub enum ConnectionType {
    Firehose,
    Substreams,
    Rpc,
}

impl Into<&str> for &ConnectionType {
    fn into(self) -> &'static str {
        match self {
            ConnectionType::Firehose => "firehose",
            ConnectionType::Substreams => "substreams",
            ConnectionType::Rpc => "rpc",
        }
    }
}

impl RequestLabels {
    fn to_slice(&self, is_success: bool) -> Box<[&str]> {
        Box::new([
            (&self.conn_type).into(),
            self.req_type.as_str(),
            self.host.as_str(),
            match is_success {
                true => "success",
                false => "failure",
            },
        ])
    }
}

/// EndpointMetrics keeps track of calls success rate for specific calls,
/// a success call to a host will clear the error count.
pub struct EndpointMetrics {
    logger: Logger,
    hosts: HostCount,
    counter: Box<IntCounterVec>,
}

impl std::fmt::Debug for EndpointMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{:?}", self.hosts))
    }
}

impl EndpointMetrics {
    pub fn new(logger: Logger, hosts: &[impl AsRef<str>], registry: Arc<MetricsRegistry>) -> Self {
        let hosts = Arc::new(HashMap::from_iter(
            hosts
                .iter()
                .map(|h| (Host::from(h.as_ref()), AtomicU64::new(0))),
        ));

        let counter = registry
            .new_int_counter_vec(
                "endpoint_request",
                "successfull request",
                &["conn_type", "req_type", "host", "result"],
            )
            .expect("unable to create endpoint_request counter_vec");

        Self {
            logger,
            hosts,
            counter,
        }
    }

    /// This should only be used for testing.
    pub fn mock() -> Self {
        use slog::{o, Discard};
        let hosts: &[&str] = &[];
        Self::new(
            Logger::root(Discard, o!()),
            hosts,
            Arc::new(MetricsRegistry::mock()),
        )
    }

    #[cfg(debug_assertions)]
    pub fn report_for_test(&self, host: &Host, success: bool) {
        match success {
            true => self.success(&RequestLabels {
                host: host.clone(),
                req_type: "".into(),
                conn_type: ConnectionType::Firehose,
            }),
            false => self.failure(&RequestLabels {
                host: host.clone(),
                req_type: "".into(),
                conn_type: ConnectionType::Firehose,
            }),
        }
    }

    pub fn success(&self, labels: &RequestLabels) {
        match self.hosts.get(&labels.host) {
            Some(count) => {
                count.store(0, Ordering::Relaxed);
            }
            None => warn!(
                &self.logger,
                "metrics not available for host {}", labels.host
            ),
        };

        self.counter.with_label_values(&labels.to_slice(true)).inc();
    }

    pub fn failure(&self, labels: &RequestLabels) {
        match self.hosts.get(&labels.host) {
            Some(count) => {
                count.fetch_add(1, Ordering::Relaxed);
            }
            None => warn!(
                &self.logger,
                "metrics not available for host {}", &labels.host
            ),
        };

        self.counter
            .with_label_values(&labels.to_slice(false))
            .inc();
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
    use std::sync::Arc;

    use slog::{o, Discard, Logger};

    use crate::{
        components::metrics::MetricsRegistry,
        endpoint::{EndpointMetrics, Host},
    };

    #[tokio::test]
    async fn should_increment_and_reset() {
        let (a, b, c): (Host, Host, Host) = ("a".into(), "b".into(), "c".into());
        let hosts: &[&str] = &[&a, &b, &c];
        let logger = Logger::root(Discard, o!());

        let metrics = EndpointMetrics::new(logger, hosts, Arc::new(MetricsRegistry::mock()));

        metrics.report_for_test(&a, true);
        metrics.report_for_test(&a, false);
        metrics.report_for_test(&b, false);
        metrics.report_for_test(&b, false);
        metrics.report_for_test(&c, true);

        assert_eq!(metrics.get_count(&a), 1);
        assert_eq!(metrics.get_count(&b), 2);
        assert_eq!(metrics.get_count(&c), 0);
    }
}
