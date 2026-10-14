use std::sync::{Mutex, atomic::AtomicBool, atomic::Ordering};
use std::time::Instant;

use crate::derive::CheapClone;
use crate::prelude::*;
use crate::telemetry::{self, AttrValue, Context, Span, SpanGroup};

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
/// ```
///
/// When tracing is enabled (see [`crate::telemetry`]), sections also
/// produce spans. A section started with [`StopwatchMetrics::start_root_section`]
/// begins a new trace; a section started while one of its ancestors on the
/// section stack has a span becomes a child of the nearest such ancestor.
/// Sections outside any root, like the base `unknown` section, have no
/// span. Parenting comes from the section stack alone, never from the
/// thread-local context, so it works unchanged across the threads that
/// share a `StopwatchMetrics`.
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
            spans: Vec::new(),
            timer: Instant::now(),
        };

        // Start a base section so that all time is accounted for.
        inner.start_section("unknown".to_owned(), SpanStart::Child, []);

        StopwatchMetrics {
            disabled: Arc::new(AtomicBool::new(false)),
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    pub fn start_section(&self, id: &str) -> Section {
        self.start_section_with(id, [])
    }

    /// Like [`Self::start_section`], with `attrs` set on the section's span
    /// if it gets one.
    pub fn start_section_with(
        &self,
        id: &str,
        attrs: impl IntoIterator<Item = (&'static str, AttrValue)>,
    ) -> Section {
        self.start(id, SpanStart::Child, attrs)
    }

    /// Start a section whose span, if `group` is enabled, is the root of a
    /// new trace rather than a child of the enclosing section's span.
    /// `links` point the new trace at other traces, for work that was
    /// caused by several of them.
    pub fn start_root_section(
        &self,
        id: &str,
        group: SpanGroup,
        attrs: impl IntoIterator<Item = (&'static str, AttrValue)>,
        links: &[Context],
    ) -> Section {
        self.start(id, SpanStart::Root { group, links }, attrs)
    }

    fn start(
        &self,
        id: &str,
        span: SpanStart<'_>,
        attrs: impl IntoIterator<Item = (&'static str, AttrValue)>,
    ) -> Section {
        let id = id.to_owned();
        if !self.disabled.load(Ordering::SeqCst) {
            let mut inner = self.inner.lock().unwrap();
            // `disable` ends all spans under the lock; a section that lost
            // the race to it must not start a new one.
            if !self.disabled.load(Ordering::SeqCst) {
                inner.start_section(id.clone(), span, attrs)
            }
        }

        // If disabled, this will do nothing on drop.
        Section {
            id,
            stopwatch: self.clone(),
        }
    }

    /// Set an attribute on the span of the current section, for details
    /// that are only known after the section started. Does nothing if the
    /// current section has no span.
    pub fn set_attr(&self, key: &'static str, value: impl Into<AttrValue>) {
        if telemetry::enabled() && !self.disabled.load(Ordering::SeqCst) {
            self.inner.lock().unwrap().set_attr(key, value.into())
        }
    }

    /// The context of the span that a section started now would be
    /// parented to, if any. Used to link work that happens elsewhere, like
    /// a queued write, back to the current trace.
    pub fn current_context(&self) -> Option<Context> {
        if telemetry::enabled() && !self.disabled.load(Ordering::SeqCst) {
            self.inner.lock().unwrap().current_context()
        } else {
            None
        }
    }

    /// Turns `start_section` and `end_section` into no-ops, no more metrics will be updated.
    /// Any live spans are ended.
    pub fn disable(&self) {
        self.disabled.store(true, Ordering::SeqCst);
        self.inner.lock().unwrap().end_spans();
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

/// How the span of a new section, if any, relates to the trace.
enum SpanStart<'a> {
    /// A child of the nearest enclosing section that has a span.
    Child,
    /// The root of a new trace, if `group` is enabled.
    Root {
        group: SpanGroup,
        links: &'a [Context],
    },
}

/// What a section on the stack contributes to the trace.
enum SectionSpan {
    /// No span; sections nested in it attach to the nearest span further
    /// down the stack.
    None,
    /// A root that got no span because its group is off or its trace was
    /// not sampled. Sections nested in it get no span either, rather than
    /// escaping into an enclosing trace.
    Boundary,
    Span(Span),
}

/// The group that decides whether a non-root section gets a span, for
/// the sections that have one of their own. Any other section follows its
/// enclosing root.
fn section_group(id: &str) -> Option<SpanGroup> {
    if id.starts_with("host_export_") {
        Some(SpanGroup::HostFn)
    } else if id == "run_handler" || id == "match_and_decode" {
        Some(SpanGroup::Handler)
    } else {
        None
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

    // The span of each section in `section_stack`, in lockstep with it.
    spans: Vec<SectionSpan>,

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

    fn start_section(
        &mut self,
        id: String,
        span: SpanStart<'_>,
        attrs: impl IntoIterator<Item = (&'static str, AttrValue)>,
    ) {
        #[cfg(debug_assertions)]
        self.record_section_relation(&id);
        self.record_and_reset();
        let span = self.start_span(&id, span, attrs);
        self.section_stack.push(id);
        self.spans.push(span);
    }

    fn start_span(
        &self,
        id: &str,
        span: SpanStart<'_>,
        attrs: impl IntoIterator<Item = (&'static str, AttrValue)>,
    ) -> SectionSpan {
        if !telemetry::enabled() {
            return SectionSpan::None;
        }

        let mut span = match span {
            SpanStart::Root { group, links } => {
                if !telemetry::group_enabled(group) {
                    return SectionSpan::Boundary;
                }
                let span = Span::root_with_links(id.to_owned(), links);
                // An unsampled root is a no-op span; its subtree is skipped.
                if !span.is_recording() {
                    return SectionSpan::Boundary;
                }
                span
            }
            SpanStart::Child => {
                let Some(parent) = self.current_context() else {
                    return SectionSpan::None;
                };
                if let Some(group) = section_group(id)
                    && !telemetry::group_enabled(group)
                {
                    return SectionSpan::None;
                }
                let span = Span::child(id.to_owned(), &parent);
                if !span.is_recording() {
                    return SectionSpan::None;
                }
                span
            }
        };

        span.set_attrs(attrs);
        SectionSpan::Span(span)
    }

    /// The context of the nearest section on the stack that has a span,
    /// stopping at a root that has none.
    fn current_context(&self) -> Option<Context> {
        for span in self.spans.iter().rev() {
            match span {
                SectionSpan::Span(span) => return span.context(),
                SectionSpan::Boundary => return None,
                SectionSpan::None => continue,
            }
        }
        None
    }

    fn set_attr(&mut self, key: &'static str, value: AttrValue) {
        if let Some(SectionSpan::Span(span)) = self.spans.last_mut() {
            span.set_attr(key, value);
        }
    }

    fn end_section(&mut self, id: String) {
        // Validate that the expected section is running.
        match self.section_stack.last() {
            Some(current_section) if current_section == &id => {
                self.record_and_reset();
                self.section_stack.pop();
                if let Some(SectionSpan::Span(span)) = self.spans.pop() {
                    span.end();
                }
            }
            Some(current_section) => error!(self.logger, "`end_section` with mismatched section";
                                                        "current" => current_section,
                                                        "received" => id),
            None => error!(self.logger, "`end_section` with no current section";
                                        "received" => id),
        }
    }

    /// End every live span, innermost first. The section stack and the
    /// span vector keep their length so they stay in lockstep.
    fn end_spans(&mut self) {
        for slot in self.spans.iter_mut().rev() {
            if let SectionSpan::Span(span) = std::mem::replace(slot, SectionSpan::None) {
                span.end();
            }
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
                .truncate(true)
                .open(section_map)
                .expect("can open file");
            serde_json::to_writer(&file, &entries).expect("can write json");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::telemetry::SpanGroups;
    use crate::telemetry::testing::{ExportedSpan, in_subprocess, install, install_with};

    fn stopwatch() -> StopwatchMetrics {
        StopwatchMetrics::new(
            crate::log::discard(),
            DeploymentHash::new("QmStopwatchSpans").unwrap(),
            "test",
            Arc::new(MetricsRegistry::mock()),
            "shard".to_string(),
        )
    }

    fn test_attr(name: &'static str) -> (&'static str, AttrValue) {
        ("test", AttrValue::from(name))
    }

    fn names(spans: &[ExportedSpan]) -> Vec<&str> {
        spans.iter().map(|span| span.name.as_str()).collect()
    }

    #[test]
    fn sections_become_nested_spans() {
        let handle = install();
        let stopwatch = stopwatch();

        // Sections outside a root have no span.
        let outside = stopwatch.start_section("scan_blocks");
        assert!(stopwatch.current_context().is_none());
        outside.end();

        let root = stopwatch.start_root_section(
            "process_block",
            SpanGroup::Block,
            [
                test_attr("sections_become_nested_spans"),
                ("block_number", 7.into()),
            ],
            &[],
        );
        let root_ctx = stopwatch.current_context().expect("root has a context");

        let triggers = stopwatch.start_section("process_triggers");
        let handler = stopwatch.start_section_with("run_handler", [("handler", "handleX".into())]);
        stopwatch.set_attr("tx_hash", "0xabc");
        let host_fn = stopwatch.start_section("host_export_store_set");
        host_fn.end();
        handler.end();
        triggers.end();
        let transact = stopwatch.start_section("transact_block");
        transact.end();
        root.end();
        assert!(stopwatch.current_context().is_none());

        let roots = handle.spans_with_attr("test", "sections_become_nested_spans");
        assert_eq!(roots.len(), 1);
        let root = &roots[0];
        assert_eq!(root.name, "process_block");
        assert_eq!(root.parent_span_id, None);
        assert_eq!(root.trace_id, root_ctx.trace_id());
        assert_eq!(root.attr("block_number"), Some("7"));

        // Five spans in the trace, none for `scan_blocks` or `unknown`.
        let trace = handle.trace(&root.trace_id);
        assert_eq!(trace.len(), 5);

        let children = handle.children_of(root);
        assert_eq!(names(&children), ["process_triggers", "transact_block"]);
        let handlers = handle.children_of(&children[0]);
        assert_eq!(names(&handlers), ["run_handler"]);
        assert_eq!(handlers[0].attr("handler"), Some("handleX"));
        assert_eq!(handlers[0].attr("tx_hash"), Some("0xabc"));
        let host_fns = handle.children_of(&handlers[0]);
        assert_eq!(names(&host_fns), ["host_export_store_set"]);
    }

    #[test]
    fn root_sections_can_link_to_other_traces() {
        let handle = install();
        let stopwatch = stopwatch();

        let mut linked_to = Vec::new();
        for _ in 0..2 {
            let block = stopwatch.start_root_section(
                "process_block",
                SpanGroup::Block,
                [test_attr("root_sections_can_link_to_other_traces")],
                &[],
            );
            linked_to.push(stopwatch.current_context().unwrap());
            block.end();
        }

        let write = stopwatch.start_root_section(
            "write_batch",
            SpanGroup::Store,
            [test_attr("root_sections_can_link_to_other_traces")],
            &linked_to,
        );
        let write_ctx = stopwatch.current_context().unwrap();
        write.end();

        let spans = handle.spans_with_attr("test", "root_sections_can_link_to_other_traces");
        let write = spans.iter().find(|s| s.name == "write_batch").unwrap();
        assert_eq!(write.trace_id, write_ctx.trace_id());
        assert!(linked_to.iter().all(|ctx| ctx.trace_id() != write.trace_id));
        let mut links: Vec<_> = write
            .links
            .iter()
            .map(|(trace_id, _)| trace_id.as_str())
            .collect();
        links.sort();
        let mut expected: Vec<_> = linked_to.iter().map(|ctx| ctx.trace_id()).collect();
        expected.sort();
        assert_eq!(links, expected);
    }

    #[test]
    fn mismatched_end_leaves_spans_consistent() {
        install();
        let stopwatch = stopwatch();

        let root = stopwatch.start_root_section("process_block", SpanGroup::Block, [], &[]);
        let child = stopwatch.start_section("a");
        stopwatch
            .inner
            .lock()
            .unwrap()
            .end_section("wrong".to_owned());
        {
            let inner = stopwatch.inner.lock().unwrap();
            assert_eq!(inner.section_stack, ["unknown", "process_block", "a"]);
            assert_eq!(inner.spans.len(), 3);
            assert!(matches!(inner.spans[0], SectionSpan::None));
            assert!(matches!(inner.spans[1], SectionSpan::Span(_)));
            assert!(matches!(inner.spans[2], SectionSpan::Span(_)));
        }
        child.end();
        root.end();
        {
            let inner = stopwatch.inner.lock().unwrap();
            assert_eq!(inner.section_stack, ["unknown"]);
            assert_eq!(inner.spans.len(), 1);
        }
    }

    #[test]
    fn disable_ends_live_spans() {
        let handle = install();
        let stopwatch = stopwatch();

        let root = stopwatch.start_root_section(
            "process_block",
            SpanGroup::Block,
            [test_attr("disable_ends_live_spans")],
            &[],
        );
        let child = stopwatch.start_section_with("a", [test_attr("disable_ends_live_spans")]);
        stopwatch.disable();

        let spans = handle.spans_with_attr("test", "disable_ends_live_spans");
        assert_eq!(names(&spans), ["a", "process_block"]);

        // The stack and the spans stay in lockstep.
        {
            let inner = stopwatch.inner.lock().unwrap();
            assert_eq!(inner.section_stack.len(), inner.spans.len());
            assert!(
                inner
                    .spans
                    .iter()
                    .all(|span| matches!(span, SectionSpan::None))
            );
        }

        // Nothing happens after `disable`.
        assert!(stopwatch.current_context().is_none());
        let after = stopwatch.start_section_with("after", [test_attr("disable_ends_live_spans")]);
        after.end();
        child.end();
        root.end();
        assert_eq!(
            handle
                .spans_with_attr("test", "disable_ends_live_spans")
                .len(),
            2
        );
    }

    #[test]
    fn groups_gate_their_sections() {
        in_subprocess(
            "components::metrics::stopwatch::tests::groups_gate_their_sections",
            &[],
            || {
                // Everything but host functions.
                let handle = install_with(SpanGroups::default());
                let stopwatch = stopwatch();

                let root = stopwatch.start_root_section(
                    "process_block",
                    SpanGroup::Block,
                    [test_attr("groups_gate_their_sections")],
                    &[],
                );
                let handler = stopwatch.start_section("run_handler");
                let host_fn = stopwatch.start_section("host_export_store_set");
                // A section under a gated-off section attaches to the
                // nearest ancestor with a span.
                let nested = stopwatch.start_section("under_host_fn");
                nested.end();
                host_fn.end();
                handler.end();
                root.end();

                let roots = handle.spans_with_attr("test", "groups_gate_their_sections");
                assert_eq!(roots.len(), 1);
                let trace = handle.trace(&roots[0].trace_id);
                assert_eq!(
                    names(&trace),
                    ["under_host_fn", "run_handler", "process_block"]
                );
                let handler = trace.iter().find(|s| s.name == "run_handler").unwrap();
                assert_eq!(names(&handle.children_of(handler)), ["under_host_fn"]);

                // A root in a disabled group produces no trace at all, and
                // nested inside a traced root it is a boundary: its
                // sections do not escape into the enclosing trace.
                let outer = stopwatch.start_root_section(
                    "process_block",
                    SpanGroup::Block,
                    [test_attr("groups_gate_their_sections_boundary")],
                    &[],
                );
                let outer_ctx = stopwatch.current_context().unwrap();
                let root = stopwatch.start_root_section(
                    "host_export_root",
                    SpanGroup::HostFn,
                    [test_attr("groups_gate_their_sections_boundary")],
                    &[],
                );
                assert!(stopwatch.current_context().is_none());
                let child = stopwatch.start_section_with(
                    "child",
                    [test_attr("groups_gate_their_sections_boundary")],
                );
                assert!(stopwatch.current_context().is_none());
                child.end();
                root.end();
                // Back in the outer trace.
                assert_eq!(
                    stopwatch.current_context().unwrap().trace_id(),
                    outer_ctx.trace_id()
                );
                let after = stopwatch.start_section("after");
                after.end();
                outer.end();
                let spans = handle.spans_with_attr("test", "groups_gate_their_sections_boundary");
                assert_eq!(names(&spans), ["process_block"]);
                assert_eq!(
                    names(&handle.trace(&outer_ctx.trace_id())),
                    ["after", "process_block"]
                );
            },
        );
    }

    #[test]
    fn tracing_off_keeps_sections_working() {
        in_subprocess(
            "components::metrics::stopwatch::tests::tracing_off_keeps_sections_working",
            &[],
            || {
                assert!(!telemetry::enabled());
                let stopwatch = stopwatch();
                let root = stopwatch.start_root_section(
                    "process_block",
                    SpanGroup::Block,
                    [("block_number", 1.into())],
                    &[],
                );
                assert!(stopwatch.current_context().is_none());
                let child = stopwatch.start_section_with("a", [("k", "v".into())]);
                stopwatch.set_attr("k2", "v2");
                {
                    let inner = stopwatch.inner.lock().unwrap();
                    assert_eq!(inner.section_stack, ["unknown", "process_block", "a"]);
                    assert!(
                        inner
                            .spans
                            .iter()
                            .all(|span| matches!(span, SectionSpan::None))
                    );
                }
                child.end();
                root.end();
                let inner = stopwatch.inner.lock().unwrap();
                assert_eq!(inner.section_stack, ["unknown"]);
                assert_eq!(inner.spans.len(), 1);
            },
        );
    }
}
