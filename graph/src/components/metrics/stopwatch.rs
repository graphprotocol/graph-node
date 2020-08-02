use crate::prelude::*;
use std::collections::HashMap;
use std::sync::{atomic::AtomicBool, atomic::Ordering, Mutex};
use std::time::Instant;

/// This is a "section guard", that closes the section on drop.
pub struct Section {
    id: String,
    stopwatch: StopwatchMetrics,
}

impl Section {
    /// A more readable `drop`.
    pub fn end(self) {}
}

impl Drop for Section {
    fn drop(&mut self) {
        self.stopwatch
            .end_section(std::mem::replace(&mut self.id, String::new()))
    }
}

/// Usage example:
/// ```ignore
/// // Start counting time for the "main_section".
/// let _main_section = stopwatch.start_section("main_section");
/// // do stuff...
/// // Pause timer for "main_section", start for "child_section".
/// let child_section = stopwatch.start_section("child_section");
/// // do stuff...
/// // Register time spent in "child_section", implicitly going back to "main_section".
/// section.end();
/// // do stuff...
/// // At the end of the scope `_main_section` is dropped, which is equivalent to calling
/// // `_main_section.end()`.
#[derive(Clone)]
pub struct StopwatchMetrics {
    disabled: Arc<AtomicBool>,
    inner: Arc<Mutex<StopwatchInner>>,
}

impl StopwatchMetrics {
    pub fn new(
        logger: Logger,
        subgraph_id: SubgraphDeploymentId,
        registry: Arc<dyn MetricsRegistry>,
    ) -> Self {
        let mut inner = StopwatchInner {
            total_counter: *registry
                .new_subgraph_counter(
                    "subgraph_sync_total_secs",
                    "total time spent syncing",
                    subgraph_id.as_str(),
                )
                .expect(&format!(
                    "failed to register subgraph_sync_total_secs prometheus counter for {}",
                    subgraph_id
                )),
            logger,
            subgraph_id,
            registry,
            counters: HashMap::new(),
            section_stack: Vec::new(),
            timer: Instant::now(),
        };

        // Start a base section so that all time is accounted for.
        inner.start_section("unknown".to_owned());

        StopwatchMetrics {
            disabled: Arc::new(AtomicBool::new(false)),
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    pub fn start_section(&self, id: &str) -> Section {
        let id = id.to_owned();
        if !self.disabled.load(Ordering::SeqCst) {
            self.inner.lock().unwrap().start_section(id.clone())
        }

        // If disabled, this will do nothing on drop.
        Section {
            id,
            stopwatch: self.clone(),
        }
    }

    /// Turns `start_section` and `end_section` into no-ops, no more metrics will be updated.
    pub fn disable(&self) {
        self.disabled.store(true, Ordering::SeqCst)
    }

    fn end_section(&self, id: String) {
        if !self.disabled.load(Ordering::SeqCst) {
            self.inner.lock().unwrap().end_section(id)
        }
    }
}

/// We want to account for all subgraph indexing time, based on "wall clock" time. To do this we
/// break down indexing into _sequential_ sections, and register the total time spent in each. So
/// that there is no double counting, time spent in child sections doesn't count for the parent.
struct StopwatchInner {
    logger: Logger,
    subgraph_id: SubgraphDeploymentId,
    registry: Arc<dyn MetricsRegistry>,

    // Counter for the total time the subgraph spent syncing.
    total_counter: Counter,

    // Counts the seconds spent in each section of the indexing code.
    counters: HashMap<String, Counter>,

    // The top section (last item) is the one that's currently executing.
    section_stack: Vec<String>,

    // The timer is reset whenever a section starts or ends.
    timer: Instant,
}

impl StopwatchInner {
    fn record_and_reset(&mut self) {
        if let Some(section) = self.section_stack.last() {
            // Get or create the counter.
            let counter = if let Some(counter) = self.counters.get(section) {
                counter.clone()
            } else {
                let name = format!("{}_{}_secs", self.subgraph_id, section);
                let help = format!("section {}", section);
                match self.registry.new_counter(&name, &help) {
                    Ok(counter) => {
                        self.counters.insert(section.clone(), (*counter).clone());
                        *counter
                    }
                    Err(e) => {
                        error!(self.logger, "failed to register counter";
                                            "id" => section,
                                            "error" => e.to_string());
                        return;
                    }
                }
            };

            // Register the current timer.
            let elapsed = self.timer.elapsed().as_secs_f64();
            self.total_counter.inc_by(elapsed);
            counter.inc_by(elapsed);
        }

        // Reset the timer.
        self.timer = Instant::now();
    }

    fn start_section(&mut self, id: String) {
        self.record_and_reset();
        self.section_stack.push(id);
    }

    fn end_section(&mut self, id: String) {
        // Validate that the expected section is running.
        match self.section_stack.last() {
            Some(current_section) if current_section == &id => {
                self.record_and_reset();
                self.section_stack.pop();
            }
            Some(current_section) => error!(self.logger, "`end_section` with mismatched section";
                                                        "current" => current_section,
                                                        "received" => id),
            None => error!(self.logger, "`end_section` with no current section";
                                        "received" => id),
        }
    }
}
