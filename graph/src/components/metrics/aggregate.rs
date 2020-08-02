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
    pub fn new(
        name: &str,
        subgraph: String,
        help: &str,
        registry: Arc<dyn MetricsRegistry>,
    ) -> Self {
        let make_gauge = |suffix: &str| {
            registry
                .new_subgraph_gauge(
                    &format!("{}_{}", name, suffix),
                    &format!("{} ({})", help, suffix),
                    &subgraph,
                )
                .expect(
                    format!(
                        "failed to register metric `{}_{}` for {}",
                        name, suffix, &subgraph
                    )
                    .as_str(),
                )
        };

        Aggregate {
            count: make_gauge("count"),
            sum: make_gauge("sum"),
            avg: make_gauge("avg"),
            cur: make_gauge("cur"),
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
