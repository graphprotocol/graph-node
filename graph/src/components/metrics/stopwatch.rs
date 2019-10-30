use crate::prelude::*;
use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;
use std::time::{Duration, Instant};

pub struct StopwatchMetrics {
    subgraph_id: SubgraphDeploymentId,
    stopwatch: Stopwatch,

    // Counts the seconds spent in each section of the indexing code.
    counters: BTreeMap<String, Counter>,
}

impl StopwatchMetrics {
    pub fn new(subgraph_id: SubgraphDeploymentId, logger: Logger) -> Self {
        Self {
            subgraph_id,
            stopwatch: Stopwatch {
                logger,
                inner: Arc::new(Mutex::new(StopwatchInner::new())),
            },
            counters: BTreeMap::new(),
        }
    }

    pub fn stopwatch(&self) -> Stopwatch {
        self.stopwatch.clone()
    }

    /// Register the current totals and resets the stopwatch.
    pub fn commit_and_reset(&mut self, registry: impl MetricsRegistry) {
        let mut stopwatch = match self.stopwatch.inner.try_lock() {
            Ok(stopwatch) => stopwatch,
            Err(e) => {
                error!(self.stopwatch.logger, "could not lock stopwatch for commit";
                                              "error" => e.to_string());
                return;
            }
        };

        // Increase the counters.
        for (id, time) in &stopwatch.totals {
            let counter = if let Some(counter) = self.counters.get(id) {
                counter.clone()
            } else {
                let mut name = id.clone();
                name.push_str("_secs");
                let help = format!("indexing section {}", id);
                let mut labels = HashMap::new();
                labels.insert("subgraph_id".to_owned(), (*self.subgraph_id).clone());
                match registry.new_counter(name, help, labels) {
                    Ok(counter) => {
                        self.counters.insert(id.clone(), (*counter).clone());
                        *counter
                    }
                    Err(e) => {
                        error!(self.stopwatch.logger, "failed to register counter";
                                                      "id" => id,
                                                      "error" => e.to_string());
                        return;
                    }
                }
            };
            counter.inc_by(time.as_secs_f64());
        }

        // Reset.
        stopwatch.totals.clear();
        stopwatch.section_stack.clear();

        // Start the "unknown" section so that all time is accounted for.
        stopwatch.section_start("unknown".to_owned());
    }
}

/// This is a "section guard", that closes the section on drop.
pub struct Section {
    id: String,
    stopwatch: Stopwatch,
}

impl Section {
    // A more readable `drop`.
    pub fn end(self) {}
}

impl Drop for Section {
    fn drop(&mut self) {
        self.stopwatch
            .section_end(std::mem::replace(&mut self.id, String::new()))
    }
}

#[derive(Clone)]
pub struct Stopwatch {
    logger: Logger,
    inner: Arc<Mutex<StopwatchInner>>,
}

impl Stopwatch {
    pub fn section_start(&self, id: &str) -> Section {
        let id = id.to_owned();
        match self.inner.try_lock() {
            Ok(mut stopwatch) => {
                stopwatch.section_start(id.clone());
                Section {
                    id,
                    stopwatch: self.clone(),
                }
            }
            Err(e) => {
                // The stopwatch was used in a non-sequential manner, log error and return a dummy
                // section, which will also log an error when dropped, making this a noop.
                error!(self.logger, "cannot `section_start`, stopwatch is being used concurrently";
                                "error" => e.to_string());
                Section {
                    id: String::new(),
                    stopwatch: Stopwatch {
                        logger: self.logger.clone(),
                        inner: Arc::new(Mutex::new(StopwatchInner::new())),
                    },
                }
            }
        }
    }

    fn section_end(&self, id: String) {
        match self.inner.try_lock() {
            Ok(mut stopwatch) => stopwatch.section_end(id, &self.logger),
            Err(e) => error!(self.logger, "cannot `section_end`, stopwatch is being used concurrently";
                                          "error" => e.to_string()),
        }
    }
}

/// We want to account for all subgraph indexing time, based on "wall clock" time. To do this we
/// break down indexing into _sequential_ sections, and register the total time spent in each. So
/// that there is no double counting, time spent in child sections doesn't count for the parent.
struct StopwatchInner {
    // When a section is popped, we add the time spent to its total.
    totals: BTreeMap<String, Duration>,

    // The top section is the one that's currently executing.
    section_stack: Vec<String>,

    // The timer is reset whenever a section starts or ends.
    timer: Instant,
}

impl StopwatchInner {
    fn new() -> Self {
        Self {
            totals: BTreeMap::new(),
            section_stack: Vec::new(),
            timer: Instant::now(), // Overwritten on first section start.
        }
    }

    fn record_and_reset(&mut self) {
        if let Some(current_section) = self.section_stack.last() {
            *self.totals.entry(current_section.clone()).or_default() += self.timer.elapsed();
        }
        self.timer = Instant::now();
    }

    fn section_start(&mut self, id: String) {
        self.record_and_reset();
        self.section_stack.push(id);
    }

    fn section_end(&mut self, id: String, logger: &Logger) {
        // Validate that the expected section is running.
        match self.section_stack.last() {
            Some(current_section) if current_section == &id => {
                self.record_and_reset();
                self.section_stack.pop();
            }
            Some(current_section) => error!(logger, "`section_end` with mismatched section";
                                                    "current" => current_section,
                                                    "received" => id),
            None => error!(logger, "`section_end` with no current section";
                                    "received" => id),
        }
    }
}
