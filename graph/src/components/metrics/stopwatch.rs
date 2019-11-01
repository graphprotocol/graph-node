use crate::prelude::*;
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Instant;

/// This is a "section guard", that closes the section on drop.
pub struct Section {
    id: String,
    stopwatch: StopwatchMetrics,
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
pub struct StopwatchMetrics(Arc<Mutex<StopwatchInner>>);

impl StopwatchMetrics {
    pub fn new(
        logger: Logger,
        subgraph_id: SubgraphDeploymentId,
        registry: Arc<dyn MetricsRegistry>,
    ) -> Self {
        let mut inner = StopwatchInner {
            logger,
            subgraph_id,
            registry,
            counters: HashMap::new(),
            section_stack: Vec::new(),
            timer: Instant::now(),
        };

        // Start a base section so that all time is accounted for.
        inner.section_start("unknown".to_owned());

        StopwatchMetrics(Arc::new(Mutex::new(inner)))
    }

    pub fn section_start(&self, id: &str) -> Section {
        let id = id.to_owned();
        self.0.lock().unwrap().section_start(id.clone());
        Section {
            id,
            stopwatch: self.clone(),
        }
    }

    fn section_end(&self, id: String) {
        self.0.lock().unwrap().section_end(id)
    }
}

/// We want to account for all subgraph indexing time, based on "wall clock" time. To do this we
/// break down indexing into _sequential_ sections, and register the total time spent in each. So
/// that there is no double counting, time spent in child sections doesn't count for the parent.
struct StopwatchInner {
    logger: Logger,
    subgraph_id: SubgraphDeploymentId,
    registry: Arc<dyn MetricsRegistry>,

    // Counts the seconds spent in each section of the indexing code.
    counters: HashMap<String, Counter>,

    // The top section is the one that's currently executing.
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
                let help = format!("indexing section {}", section);
                match self.registry.new_counter(name, help, HashMap::new()) {
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
            counter.inc_by(self.timer.elapsed().as_secs_f64());
        }

        // Reset the timer.
        self.timer = Instant::now();
    }

    fn section_start(&mut self, id: String) {
        self.record_and_reset();
        self.section_stack.push(id);
    }

    fn section_end(&mut self, id: String) {
        // Validate that the expected section is running.
        match self.section_stack.last() {
            Some(current_section) if current_section == &id => {
                self.record_and_reset();
                self.section_stack.pop();
            }
            Some(current_section) => error!(self.logger, "`section_end` with mismatched section";
                                                        "current" => current_section,
                                                        "received" => id),
            None => error!(self.logger, "`section_end` with no current section";
                                        "received" => id),
        }
    }
}
