use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use crate::prelude::{HistogramVec, MetricsRegistry, StopwatchMetrics};

pub struct HostMetrics {
  handler_execution_time: Box<HistogramVec>,
  host_fn_execution_time: Box<HistogramVec>,
  pub stopwatch: StopwatchMetrics,
}

impl fmt::Debug for HostMetrics {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    // TODO: `HistogramVec` does not implement fmt::Debug, what is the best way to deal with this?
    write!(f, "HostMetrics {{ }}")
  }
}

impl HostMetrics {
  pub fn new(
    registry: Arc<impl MetricsRegistry>,
    subgraph_hash: String,
    stopwatch: StopwatchMetrics,
  ) -> Self {
    let handler_execution_time = registry
      .new_histogram_vec(
        format!("subgraph_handler_execution_time_{}", subgraph_hash),
        String::from("Measures the execution time for handlers"),
        HashMap::new(),
        vec![String::from("handler")],
        vec![0.1, 0.5, 1.0, 10.0, 100.0],
      )
      .expect("failed to create `subgraph_handler_execution_time` histogram");
    let host_fn_execution_time = registry
      .new_histogram_vec(
        format!("subgraph_host_fn_execution_time_{}", subgraph_hash),
        String::from("Measures the execution time for host functions"),
        HashMap::new(),
        vec![String::from("host_fn_name")],
        vec![0.025, 0.05, 0.2, 2.0, 8.0, 20.0],
      )
      .expect("failed to create `subgraph_host_fn_execution_time` histogram");
    Self {
      handler_execution_time,
      host_fn_execution_time,
      stopwatch,
    }
  }

  pub fn observe_handler_execution_time(&self, duration: f64, handler: &str) {
    self
      .handler_execution_time
      .with_label_values(vec![handler].as_slice())
      .observe(duration);
  }

  pub fn observe_host_fn_execution_time(&self, duration: f64, fn_name: &str) {
    self
      .host_fn_execution_time
      .with_label_values(vec![fn_name].as_slice())
      .observe(duration);
  }
}
