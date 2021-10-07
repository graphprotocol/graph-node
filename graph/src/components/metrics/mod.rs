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

fn deployment_labels(subgraph: &str) -> HashMap<String, String> {
    labels! { String::from("deployment") => String::from(subgraph), }
}

/// Create an unregistered counter with labels
pub fn counter_with_labels(
    name: &str,
    help: &str,
    const_labels: HashMap<String, String>,
) -> Result<Counter, PrometheusError> {
    let opts = Opts::new(name, help).const_labels(const_labels);
    Counter::with_opts(opts)
}

/// Create an unregistered gauge with labels
pub fn gauge_with_labels(
    name: &str,
    help: &str,
    const_labels: HashMap<String, String>,
) -> Result<Gauge, PrometheusError> {
    let opts = Opts::new(name, help).const_labels(const_labels);
    Gauge::with_opts(opts)
}

pub trait MetricsRegistry: Send + Sync + 'static {
    fn register(&self, name: &str, c: Box<dyn Collector>);

    fn unregister(&self, metric: Box<dyn Collector>);

    fn global_counter(
        &self,
        name: &str,
        help: &str,
        const_labels: HashMap<String, String>,
    ) -> Result<Counter, PrometheusError>;

    fn global_counter_vec(
        &self,
        name: &str,
        help: &str,
        variable_labels: &[&str],
    ) -> Result<CounterVec, PrometheusError>;

    fn global_deployment_counter(
        &self,
        name: &str,
        help: &str,
        subgraph: &str,
    ) -> Result<Counter, PrometheusError> {
        self.global_counter(name, help, deployment_labels(subgraph))
    }

    fn global_gauge(
        &self,
        name: &str,
        help: &str,
        const_labels: HashMap<String, String>,
    ) -> Result<Gauge, PrometheusError>;

    fn new_gauge(
        &self,
        name: &str,
        help: &str,
        const_labels: HashMap<String, String>,
    ) -> Result<Box<Gauge>, PrometheusError> {
        let opts = Opts::new(name, help).const_labels(const_labels);
        let gauge = Box::new(Gauge::with_opts(opts)?);
        self.register(name, gauge.clone());
        Ok(gauge)
    }

    fn new_deployment_gauge(
        &self,
        name: &str,
        help: &str,
        subgraph: &str,
    ) -> Result<Box<Gauge>, PrometheusError> {
        let opts = Opts::new(name, help).const_labels(deployment_labels(subgraph));
        let gauge = Box::new(Gauge::with_opts(opts)?);
        self.register(name, gauge.clone());
        Ok(gauge)
    }

    fn new_gauge_vec(
        &self,
        name: &str,
        help: &str,
        variable_labels: Vec<String>,
    ) -> Result<Box<GaugeVec>, PrometheusError> {
        let opts = Opts::new(name, help);
        let gauges = Box::new(GaugeVec::new(
            opts,
            variable_labels
                .iter()
                .map(String::as_str)
                .collect::<Vec<&str>>()
                .as_slice(),
        )?);
        self.register(name, gauges.clone());
        Ok(gauges)
    }

    fn new_deployment_gauge_vec(
        &self,
        name: &str,
        help: &str,
        subgraph: &str,
        variable_labels: Vec<String>,
    ) -> Result<Box<GaugeVec>, PrometheusError> {
        let opts = Opts::new(name, help).const_labels(deployment_labels(subgraph));
        let gauges = Box::new(GaugeVec::new(
            opts,
            variable_labels
                .iter()
                .map(String::as_str)
                .collect::<Vec<&str>>()
                .as_slice(),
        )?);
        self.register(name, gauges.clone());
        Ok(gauges)
    }

    fn new_counter(&self, name: &str, help: &str) -> Result<Box<Counter>, PrometheusError> {
        let opts = Opts::new(name, help);
        let counter = Box::new(Counter::with_opts(opts)?);
        self.register(name, counter.clone());
        Ok(counter)
    }

    fn new_counter_with_labels(
        &self,
        name: &str,
        help: &str,
        const_labels: HashMap<String, String>,
    ) -> Result<Box<Counter>, PrometheusError> {
        let counter = Box::new(counter_with_labels(name, help, const_labels)?);
        self.register(name, counter.clone());
        Ok(counter)
    }

    fn new_deployment_counter(
        &self,
        name: &str,
        help: &str,
        subgraph: &str,
    ) -> Result<Box<Counter>, PrometheusError> {
        let counter = Box::new(counter_with_labels(
            name,
            help,
            deployment_labels(subgraph),
        )?);
        self.register(name, counter.clone());
        Ok(counter)
    }

    fn new_counter_vec(
        &self,
        name: &str,
        help: &str,
        variable_labels: Vec<String>,
    ) -> Result<Box<CounterVec>, PrometheusError> {
        let opts = Opts::new(name, help);
        let counters = Box::new(CounterVec::new(
            opts,
            variable_labels
                .iter()
                .map(String::as_str)
                .collect::<Vec<&str>>()
                .as_slice(),
        )?);
        self.register(name, counters.clone());
        Ok(counters)
    }

    fn new_deployment_counter_vec(
        &self,
        name: &str,
        help: &str,
        subgraph: &str,
        variable_labels: Vec<String>,
    ) -> Result<Box<CounterVec>, PrometheusError> {
        let opts = Opts::new(name, help).const_labels(deployment_labels(subgraph));
        let counters = Box::new(CounterVec::new(
            opts,
            variable_labels
                .iter()
                .map(String::as_str)
                .collect::<Vec<&str>>()
                .as_slice(),
        )?);
        self.register(name, counters.clone());
        Ok(counters)
    }

    fn new_deployment_histogram(
        &self,
        name: &str,
        help: &str,
        subgraph: &str,
        buckets: Vec<f64>,
    ) -> Result<Box<Histogram>, PrometheusError> {
        let opts = HistogramOpts::new(name, help)
            .const_labels(deployment_labels(subgraph))
            .buckets(buckets);
        let histogram = Box::new(Histogram::with_opts(opts)?);
        self.register(name, histogram.clone());
        Ok(histogram)
    }

    fn new_histogram(
        &self,
        name: &str,
        help: &str,
        buckets: Vec<f64>,
    ) -> Result<Box<Histogram>, PrometheusError> {
        let opts = HistogramOpts::new(name, help).buckets(buckets);
        let histogram = Box::new(Histogram::with_opts(opts)?);
        self.register(name, histogram.clone());
        Ok(histogram)
    }

    fn new_histogram_vec(
        &self,
        name: &str,
        help: &str,
        variable_labels: Vec<String>,
        buckets: Vec<f64>,
    ) -> Result<Box<HistogramVec>, PrometheusError> {
        let opts = Opts::new(name, help);
        let histograms = Box::new(HistogramVec::new(
            HistogramOpts {
                common_opts: opts,
                buckets,
            },
            variable_labels
                .iter()
                .map(String::as_str)
                .collect::<Vec<&str>>()
                .as_slice(),
        )?);
        self.register(name, histograms.clone());
        Ok(histograms)
    }

    fn new_deployment_histogram_vec(
        &self,
        name: &str,
        help: &str,
        subgraph: &str,
        variable_labels: Vec<String>,
        buckets: Vec<f64>,
    ) -> Result<Box<HistogramVec>, PrometheusError> {
        let opts = Opts::new(name, help).const_labels(deployment_labels(subgraph));
        let histograms = Box::new(HistogramVec::new(
            HistogramOpts {
                common_opts: opts,
                buckets,
            },
            variable_labels
                .iter()
                .map(String::as_str)
                .collect::<Vec<&str>>()
                .as_slice(),
        )?);
        self.register(name, histograms.clone());
        Ok(histograms)
    }
}
