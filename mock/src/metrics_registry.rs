use graph::components::metrics::{Collector, Counter, Gauge, Opts, PrometheusError};
use graph::prelude::MetricsRegistry as MetricsRegistryTrait;
use graph::prometheus::{CounterVec, GaugeVec, HistogramOpts, HistogramVec};

use std::collections::HashMap;

pub struct MockMetricsRegistry {}

impl MockMetricsRegistry {
    pub fn new() -> Self {
        Self {}
    }
}

impl Clone for MockMetricsRegistry {
    fn clone(&self) -> Self {
        Self {}
    }
}

impl MetricsRegistryTrait for MockMetricsRegistry {
    fn register(&self, _name: &str, _c: Box<dyn Collector>) {
        // Ignore, we do not register metrics
    }

    fn global_counter(
        &self,
        name: &str,
        help: &str,
        const_labels: HashMap<String, String>,
    ) -> Result<Counter, PrometheusError> {
        let opts = Opts::new(name, help).const_labels(const_labels);
        Counter::with_opts(opts)
    }

    fn global_gauge(
        &self,
        name: &str,
        help: &str,
        const_labels: HashMap<String, String>,
    ) -> Result<Gauge, PrometheusError> {
        let opts = Opts::new(name, help).const_labels(const_labels);
        Gauge::with_opts(opts)
    }

    fn unregister(&self, _: Box<dyn Collector>) {}

    fn global_counter_vec(
        &self,
        name: &str,
        help: &str,
        variable_labels: &[&str],
    ) -> Result<CounterVec, PrometheusError> {
        let opts = Opts::new(name, help);
        let counters = CounterVec::new(opts, variable_labels)?;
        Ok(counters)
    }

    fn global_deployment_counter_vec(
        &self,
        name: &str,
        help: &str,
        subgraph: &str,
        variable_labels: &[&str],
    ) -> Result<CounterVec, PrometheusError> {
        let opts = Opts::new(name, help).const_label("deployment", subgraph);
        let counters = CounterVec::new(opts, variable_labels)?;
        Ok(counters)
    }

    fn global_gauge_vec(
        &self,
        name: &str,
        help: &str,
        variable_labels: &[&str],
    ) -> Result<GaugeVec, PrometheusError> {
        let opts = Opts::new(name, help);
        let gauges = GaugeVec::new(opts, variable_labels)?;
        Ok(gauges)
    }

    fn global_histogram_vec(
        &self,
        name: &str,
        help: &str,
        variable_labels: &[&str],
    ) -> Result<HistogramVec, PrometheusError> {
        let opts = HistogramOpts::new(name, help);
        let histograms = HistogramVec::new(opts, variable_labels)?;
        Ok(histograms)
    }
}
