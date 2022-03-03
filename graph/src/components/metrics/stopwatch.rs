use std::sync::{atomic::AtomicBool, atomic::Ordering, Mutex};
use std::time::Instant;

use crate::derive::CheapClone;
use crate::prelude::*;

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
        self.stopwatch.end_section(std::mem::take(&mut self.id))
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
#[derive(Clone, CheapClone)]
pub struct StopwatchMetrics {
    disabled: Arc<AtomicBool>,
    inner: Arc<Mutex<StopwatchInner>>,
}

impl StopwatchMetrics {
    pub fn new(
        logger: Logger,
        subgraph_id: DeploymentHash,
        stage: &str,
        registry: Arc<MetricsRegistry>,
        shard: String,
    ) -> Self {
        let stage = stage.to_owned();
        let mut inner = StopwatchInner {
            counter: registry
                .global_deployment_counter_vec(
                    "deployment_sync_secs",
                    "total time spent syncing",
                    subgraph_id.as_str(),
                    &["section", "stage", "shard"],
                )
                .unwrap_or_else(|_| {
                    panic!(
                        "failed to register subgraph_sync_total_secs prometheus counter for {}",
                        subgraph_id
                    )
                }),
            logger,
            stage,
            shard,
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

    pub fn shard(&self) -> String {
        self.inner.lock().unwrap().shard.to_string()
    }
}

/// We want to account for all subgraph indexing time, based on "wall clock" time. To do this we
/// break down indexing into _sequential_ sections, and register the total time spent in each. So
/// that there is no double counting, time spent in child sections doesn't count for the parent.
struct StopwatchInner {
    logger: Logger,

    // Counter for the total time the subgraph spent syncing in various sections.
    counter: CounterVec,

    // The top section (last item) is the one that's currently executing.
    section_stack: Vec<String>,

    // The timer is reset whenever a section starts or ends.
    timer: Instant,

    // The processing stage the metrics belong to; for pipelined uses, the
    // pipeline stage
    stage: String,

    shard: String,
}

impl StopwatchInner {
    fn record_and_reset(&mut self) {
        if let Some(section) = self.section_stack.last() {
            // Register the current timer.
            let elapsed = self.timer.elapsed().as_secs_f64();
            self.counter
                .get_metric_with_label_values(&[section, &self.stage, &self.shard])
                .map(|counter| counter.inc_by(elapsed))
                .unwrap_or_else(|e| {
                    error!(self.logger, "failed to find counter for section";
                    "id" => section,
                    "error" => e.to_string());
                });
        }

        // Reset the timer.
        self.timer = Instant::now();
    }

    fn start_section(&mut self, id: String) {
        #[cfg(debug_assertions)]
        self.record_section_relation(&id);
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

    /// In debug builds, allow recording the relation between sections to
    /// build a tree of how sections are nested. The resulting JSON file can
    /// be turned into a graph with Graphviz's `dot` command using this
    /// shell script:
    ///
    /// ```sh
    /// #! /bin/bash
    ///
    /// src=/tmp/sections.txt # GRAPH_SECTION_MAP
    /// out=/tmp/sections.dot
    ///
    /// echo 'digraph { node [shape="box"];' > $out
    /// jq -r '.[] | "\"\(.parent)[\(.stage)]\" -> \"\(.child)[\(.stage)]\";"' $src >> $out
    /// echo "}" >> $out
    ///
    /// dot -Tpng -O $out
    /// ```
    #[cfg(debug_assertions)]
    fn record_section_relation(&self, child: &str) {
        use std::fs;
        use std::fs::OpenOptions;

        lazy_static! {
            static ref FILE_LOCK: Mutex<()> = Mutex::new(());
        }

        #[derive(PartialEq, Serialize, Deserialize)]
        struct Entry {
            parent: String,
            child: String,
            stage: String,
        }

        if let Some(section_map) = &ENV_VARS.section_map {
            let _guard = FILE_LOCK.lock().unwrap();
            let prev = self
                .section_stack
                .last()
                .map(|s| s.as_str())
                .unwrap_or("none");

            let mut entries: Vec<Entry> = match fs::read_to_string(section_map) {
                Ok(existing) => serde_json::from_str(&existing).expect("can parse json"),
                Err(_) => Vec::new(),
            };
            let new_entry = Entry {
                parent: prev.to_string(),
                child: child.to_string(),
                stage: self.stage.to_string(),
            };
            if !entries.contains(&new_entry) {
                entries.push(new_entry);
            }
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .append(false)
                .create(true)
                .open(section_map)
                .expect("can open file");
            serde_json::to_writer(&file, &entries).expect("can write json");
        }
    }
}
