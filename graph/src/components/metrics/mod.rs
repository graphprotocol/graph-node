pub use prometheus::core::Collector;
pub use prometheus::{
    labels, Counter, CounterVec, Error as PrometheusError, Gauge, GaugeVec, Histogram,
    HistogramOpts, HistogramVec, Opts, Registry,
};
use std::collections::HashMap;

/// Metrics for measuring where time is spent during indexing.
pub mod stopwatch;

/// Aggregates over individual values.
pub mod aggregate;

pub trait MetricsRegistry: Send + Sync + 'static {
    fn new_gauge(
        &self,
        name: &str,
        help: &str,
        const_labels: HashMap<String, String>,
    ) -> Result<Box<Gauge>, PrometheusError>;

    fn new_gauge_vec(
        &self,
        name: &str,
        help: &str,
        const_labels: HashMap<String, String>,
        variable_labels: Vec<String>,
    ) -> Result<Box<GaugeVec>, PrometheusError>;

    fn new_counter(
        &self,
        name: &str,
        help: &str,
        const_labels: HashMap<String, String>,
    ) -> Result<Box<Counter>, PrometheusError>;

    fn global_counter(
        &self,
        name: &str,
        help: &str,
        const_labels: HashMap<String, String>,
    ) -> Result<Counter, PrometheusError>;

    fn global_gauge(
        &self,
        name: &str,
        help: &str,
        const_labels: HashMap<String, String>,
    ) -> Result<Gauge, PrometheusError>;

    fn new_counter_vec(
        &self,
        name: &str,
        help: &str,
        const_labels: HashMap<String, String>,
        variable_labels: Vec<String>,
    ) -> Result<Box<CounterVec>, PrometheusError>;

    fn new_histogram(
        &self,
        name: &str,
        help: &str,
        const_labels: HashMap<String, String>,
        buckets: Vec<f64>,
    ) -> Result<Box<Histogram>, PrometheusError>;

    fn new_histogram_vec(
        &self,
        name: &str,
        help: &str,
        const_labels: HashMap<String, String>,
        variable_labels: Vec<String>,
        buckets: Vec<f64>,
    ) -> Result<Box<HistogramVec>, PrometheusError>;

    fn unregister(&self, metric: Box<dyn Collector>);

    fn subgraph_labels(&self, subgraph: &str) -> HashMap<String, String> {
        labels! { String::from("subgraph") => String::from(subgraph), }
    }
}
