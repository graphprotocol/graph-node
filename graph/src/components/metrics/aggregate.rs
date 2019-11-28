use std::collections::HashMap;
use std::time::Duration;

use crate::prelude::*;

pub struct Aggregate {
    /// Number of values.
    count: Box<Gauge>,

    /// Sum over all values.
    sum: Box<Gauge>,

    /// Moving average over the values.
    avg: Box<Gauge>,

    /// Latest value.
    cur: Box<Gauge>,
}

impl Aggregate {
    pub fn new(name: String, help: &str, registry: Arc<dyn MetricsRegistry>) -> Self {
        let count = registry
            .new_gauge(
                format!("{}_count", name),
                format!("{} (count)", help),
                HashMap::new(),
            )
            .expect(format!("failed to register metric `{}_count`", name).as_str());

        let sum = registry
            .new_gauge(
                format!("{}_sum", name),
                format!("{} (sum)", help),
                HashMap::new(),
            )
            .expect(format!("failed to register metric `{}_sum`", name).as_str());

        let avg = registry
            .new_gauge(
                format!("{}_avg", name),
                format!("{} (avg)", help),
                HashMap::new(),
            )
            .expect(format!("failed to register metric `{}_avg`", name).as_str());

        let cur = registry
            .new_gauge(
                format!("{}_cur", name),
                format!("{} (cur)", help),
                HashMap::new(),
            )
            .expect(format!("failed to register metric `{}_cur`", name).as_str());

        Aggregate {
            count,
            sum,
            avg,
            cur,
        }
    }

    pub fn update(&self, x: f64) {
        // Update count
        self.count.inc();
        let n = self.count.get();

        // Update sum
        self.sum.add(x);

        // Update current value
        self.cur.set(x);

        // Update aggregate value.
        let avg = self.avg.get();
        self.avg.set(avg + (x - avg) / n);
    }

    pub fn update_duration(&self, x: Duration) {
        self.update(x.as_secs_f64())
    }
}
