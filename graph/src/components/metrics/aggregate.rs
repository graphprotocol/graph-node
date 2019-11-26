use std::collections::HashMap;
use std::time::Duration;

use crate::prelude::*;

pub struct Aggregate {
    /// Number of values.
    n: u64,

    /// Sum over all values.
    sum: f64,

    /// Moving average over the values.
    avg: f64,

    /// Latest value.
    cur: f64,

    /// Metrics for Prometheus.
    count_gauge: Box<Gauge>,
    sum_gauge: Box<Gauge>,
    avg_gauge: Box<Gauge>,
    cur_gauge: Box<Gauge>,
}

impl Aggregate {
    pub fn new(name: String, help: &str, registry: Arc<dyn MetricsRegistry>) -> Self {
        let count_gauge = registry
            .new_gauge(
                format!("{}_count", name),
                format!("{} (count)", help),
                HashMap::new(),
            )
            .expect(format!("failed to register metric `{}_count`", name).as_str());

        let sum_gauge = registry
            .new_gauge(
                format!("{}_sum", name),
                format!("{} (sum)", help),
                HashMap::new(),
            )
            .expect(format!("failed to register metric `{}_sum`", name).as_str());

        let avg_gauge = registry
            .new_gauge(
                format!("{}_avg", name),
                format!("{} (avg)", help),
                HashMap::new(),
            )
            .expect(format!("failed to register metric `{}_avg`", name).as_str());
        let cur_gauge = registry
            .new_gauge(
                format!("{}_cur", name),
                format!("{} (cur)", help),
                HashMap::new(),
            )
            .expect(format!("failed to register metric `{}_cur`", name).as_str());

        Aggregate {
            n: 0,
            sum: 0.0,
            avg: 0.0,
            cur: 0.0,
            count_gauge,
            sum_gauge,
            avg_gauge,
            cur_gauge,
        }
    }

    pub fn update(&mut self, x: f64) {
        // Update aggregate values.
        self.n += 1;
        self.sum += x;
        self.cur = x;
        self.avg = self.avg + (x - self.avg) / (self.n as f64);

        // Update gauges
        self.count_gauge.set(self.n as f64);
        self.sum_gauge.set(self.sum);
        self.avg_gauge.set(self.avg);
        self.cur_gauge.set(self.cur);
    }

    pub fn update_duration(&mut self, x: Duration) {
        self.update(x.as_secs_f64())
    }
}
