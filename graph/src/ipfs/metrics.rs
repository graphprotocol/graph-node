use std::sync::Arc;

use prometheus::{HistogramVec, IntCounterVec};

use crate::{components::metrics::MetricsRegistry, derive::CheapClone};

#[derive(Debug, Clone, CheapClone)]
pub struct IpfsMetrics {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    request_count: Box<IntCounterVec>,
    error_count: Box<IntCounterVec>,
    not_found_count: Box<IntCounterVec>,
    request_duration: Box<HistogramVec>,
}

impl IpfsMetrics {
    pub fn new(registry: &MetricsRegistry) -> Self {
        let request_count = registry
            .new_int_counter_vec(
                "ipfs_request_count",
                "The total number of IPFS requests.",
                &["deployment"],
            )
            .unwrap();

        let error_count = registry
            .new_int_counter_vec(
                "ipfs_error_count",
                "The total number of failed IPFS requests.",
                &["deployment"],
            )
            .unwrap();

        let not_found_count = registry
            .new_int_counter_vec(
                "ipfs_not_found_count",
                "The total number of IPFS requests that timed out.",
                &["deployment"],
            )
            .unwrap();

        let request_duration = registry
            .new_histogram_vec(
                "ipfs_request_duration",
                "The duration of successful IPFS requests.\n\
                 The time it takes to download the response body is not included.",
                vec!["deployment".to_owned()],
                vec![
                    0.2, 0.5, 1.0, 5.0, 10.0, 20.0, 30.0, 60.0, 90.0, 120.0, 180.0, 240.0,
                ],
            )
            .unwrap();

        Self {
            inner: Arc::new(Inner {
                request_count,
                error_count,
                not_found_count,
                request_duration,
            }),
        }
    }

    pub(super) fn add_request(&self, deployment_hash: &str) {
        self.inner
            .request_count
            .with_label_values(&[deployment_hash])
            .inc()
    }

    pub(super) fn add_error(&self, deployment_hash: &str) {
        self.inner
            .error_count
            .with_label_values(&[deployment_hash])
            .inc()
    }

    pub(super) fn add_not_found(&self, deployment_hash: &str) {
        self.inner
            .not_found_count
            .with_label_values(&[deployment_hash])
            .inc()
    }

    pub(super) fn observe_request_duration(&self, deployment_hash: &str, duration_secs: f64) {
        self.inner
            .request_duration
            .with_label_values(&[deployment_hash])
            .observe(duration_secs.clamp(0.2, 240.0));
    }
}

#[cfg(debug_assertions)]
impl Default for IpfsMetrics {
    fn default() -> Self {
        Self::new(&MetricsRegistry::mock())
    }
}
