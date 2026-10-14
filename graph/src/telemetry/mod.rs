//! Opt-in OpenTelemetry tracing.
//!
//! This module is the only place in graph-node that uses the `opentelemetry`
//! crates. Everything else goes through the small API defined here, so that
//! span creation is a cheap no-op unless tracing was enabled at startup and
//! so that a change in the OpenTelemetry API only has to be dealt with in
//! one place.
//!
//! Spans are organised in [`SpanGroup`]s. The expensive groups, like one
//! span per WASM host function call, can be left off while blocks and
//! handlers are still traced. Which groups are on is decided once at
//! startup; see [`SpanGroups`].
//!
//! Nothing here installs a tracer provider. That is done by the node at
//! startup, and by [`testing::install`] in tests. Until one of them runs,
//! [`enabled`] is `false` and every [`Span`] is a no-op.

mod span;
pub mod testing;

use std::fmt;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};

use opentelemetry::global;
use opentelemetry::trace::{SpanContext, TraceContextExt};
use opentelemetry_http::{HeaderExtractor, HeaderInjector};

pub use span::{AttrValue, Span};

/// Whether a tracer provider has been installed. Checked before any span
/// work so that the disabled path costs one atomic load.
static ENABLED: AtomicBool = AtomicBool::new(false);

/// The bitset of enabled [`SpanGroup`]s. Only meaningful when `ENABLED`
/// is set; it is written before `ENABLED` and read after it.
static GROUPS: AtomicU8 = AtomicU8::new(0);

/// `true` once a tracer provider has been installed.
#[inline]
pub fn enabled() -> bool {
    ENABLED.load(Ordering::Acquire)
}

/// `true` if tracing is enabled and `group` was selected.
#[inline]
pub fn group_enabled(group: SpanGroup) -> bool {
    enabled() && GROUPS.load(Ordering::Relaxed) & group.bit() != 0
}

/// Flip the global switch. Called by whoever installs a tracer provider.
fn set_enabled(groups: SpanGroups) {
    GROUPS.store(groups.0, Ordering::Relaxed);
    ENABLED.store(true, Ordering::Release);
}

/// A family of spans that can be turned on and off together.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SpanGroup {
    /// One root span per processed block and the runner sections under it.
    Block,
    /// `run_handler` and `match_and_decode`, one per trigger.
    Handler,
    /// One span per WASM host function call. Can be thousands per handler.
    HostFn,
    /// Write batches in the store and the SQL queries that serve GraphQL.
    Store,
    /// One root span per GraphQL request.
    GraphQl,
}

impl SpanGroup {
    const ALL: [SpanGroup; 5] = [
        SpanGroup::Block,
        SpanGroup::Handler,
        SpanGroup::HostFn,
        SpanGroup::Store,
        SpanGroup::GraphQl,
    ];

    /// The name used in `GRAPH_OTEL_SPANS`.
    pub fn name(self) -> &'static str {
        match self {
            SpanGroup::Block => "block",
            SpanGroup::Handler => "handler",
            SpanGroup::HostFn => "host_fn",
            SpanGroup::Store => "store",
            SpanGroup::GraphQl => "graphql",
        }
    }

    fn from_name(name: &str) -> Option<SpanGroup> {
        Self::ALL.into_iter().find(|group| group.name() == name)
    }

    fn bit(self) -> u8 {
        1 << (self as u8)
    }
}

/// A set of [`SpanGroup`]s, parsed from `GRAPH_OTEL_SPANS`.
///
/// The variable is a comma-separated list of group names. `all` and `none`
/// are accepted as shorthands. Unknown names are an error so that a typo
/// does not silently disable a group.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct SpanGroups(u8);

impl SpanGroups {
    pub const NONE: SpanGroups = SpanGroups(0);
    pub const ALL: SpanGroups = SpanGroups(0b11111);

    pub fn contains(self, group: SpanGroup) -> bool {
        self.0 & group.bit() != 0
    }

    pub fn with(self, group: SpanGroup) -> SpanGroups {
        SpanGroups(self.0 | group.bit())
    }

    pub fn iter(self) -> impl Iterator<Item = SpanGroup> {
        SpanGroup::ALL
            .into_iter()
            .filter(move |group| self.contains(*group))
    }
}

impl Default for SpanGroups {
    /// Everything except `host_fn`, which is too verbose to be on by default.
    fn default() -> Self {
        SpanGroups::NONE
            .with(SpanGroup::Block)
            .with(SpanGroup::Handler)
            .with(SpanGroup::Store)
            .with(SpanGroup::GraphQl)
    }
}

impl FromStr for SpanGroups {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut groups = SpanGroups::NONE;
        for name in s.split(',').map(str::trim).filter(|name| !name.is_empty()) {
            match name {
                "all" => groups = SpanGroups::ALL,
                "none" => groups = SpanGroups::NONE,
                name => match SpanGroup::from_name(name) {
                    Some(group) => groups = groups.with(group),
                    None => {
                        let known = SpanGroup::ALL
                            .iter()
                            .map(|group| group.name())
                            .collect::<Vec<_>>()
                            .join(", ");
                        return Err(format!(
                            "unknown span group `{name}`, expected one of {known}, all, none"
                        ));
                    }
                },
            }
        }
        Ok(groups)
    }
}

impl fmt::Display for SpanGroups {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let names: Vec<_> = self.iter().map(SpanGroup::name).collect();
        write!(f, "{}", names.join(","))
    }
}

impl fmt::Debug for SpanGroups {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SpanGroups({self})")
    }
}

/// The identity of a span, used to start children of it and to carry a
/// trace across threads and tasks.
///
/// A `Context` is only ever obtained from a live, sampled [`Span`] or from
/// an incoming request header, so holding one implies there is something
/// to attach to.
#[derive(Clone, Debug)]
pub struct Context(opentelemetry::Context);

impl Context {
    fn from_span_context(span_context: SpanContext) -> Context {
        Context(opentelemetry::Context::new().with_remote_span_context(span_context))
    }

    /// The context attached to the current thread, if any.
    ///
    /// Only code that runs on one thread for the lifetime of a [`ContextGuard`]
    /// should rely on this. Everywhere else, pass a `Context` explicitly.
    pub fn current() -> Option<Context> {
        if !enabled() {
            return None;
        }
        let cx = opentelemetry::Context::current();
        if cx.span().span_context().is_valid() {
            Some(Context(cx))
        } else {
            None
        }
    }

    /// Make this the current context of the calling thread until the guard
    /// is dropped. The guard must be dropped on the thread that created it
    /// and must not be held across an `await`.
    pub fn attach(self) -> ContextGuard {
        ContextGuard(self.0.attach())
    }

    fn span_context(&self) -> SpanContext {
        self.0.span().span_context().clone()
    }

    /// The trace id as a hex string, for logging and tests.
    pub fn trace_id(&self) -> String {
        self.span_context().trace_id().to_string()
    }
}

/// Restores the previous thread-local context on drop. See
/// [`Context::attach`].
#[must_use = "dropping the guard detaches the context"]
pub struct ContextGuard(#[allow(dead_code)] opentelemetry::ContextGuard);

/// Read the W3C trace context headers from an incoming request.
///
/// Returns `None` when tracing is off or the request carries no valid
/// `traceparent` header.
pub fn extract(headers: &http::HeaderMap) -> Option<Context> {
    if !enabled() {
        return None;
    }
    let cx = global::get_text_map_propagator(|propagator| {
        propagator.extract_with_context(&opentelemetry::Context::new(), &HeaderExtractor(headers))
    });
    if cx.span().span_context().is_valid() {
        Some(Context(cx))
    } else {
        None
    }
}

/// Write W3C trace context headers for `ctx` into an outgoing request.
pub fn inject(ctx: &Context, headers: &mut http::HeaderMap) {
    if !enabled() {
        return;
    }
    global::get_text_map_propagator(|propagator| {
        propagator.inject_context(&ctx.0, &mut HeaderInjector(headers))
    });
}

#[cfg(test)]
mod tests {
    use std::process::Command;

    use http::{HeaderMap, HeaderValue};

    use super::testing::install;
    use super::*;

    const TRACEPARENT_SAMPLED: &str = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
    const TRACEPARENT_UNSAMPLED: &str = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-00";

    /// Marks a child process spawned by `in_subprocess`.
    const CHILD_MARKER: &str = "GRAPH_TELEMETRY_TEST_CHILD";

    /// Run `body` in a fresh process with the telemetry environment
    /// variables unset. The tracer provider and the enabled flag are
    /// process-global, so this is the only way to observe the disabled
    /// state without racing other tests in this binary.
    ///
    /// The parent re-executes the test binary for the single test named
    /// `test_name` with `CHILD_MARKER` set; the child sees the marker and
    /// runs `body` instead of spawning again.
    fn in_subprocess(test_name: &str, body: impl FnOnce()) {
        if std::env::var_os(CHILD_MARKER).is_some() {
            body();
            return;
        }

        let exe = std::env::current_exe().unwrap();
        let output = Command::new(exe)
            .args(["--exact", test_name, "--test-threads=1"])
            .env(CHILD_MARKER, "1")
            .env_remove("GRAPH_OTEL_TRACING")
            .env_remove("GRAPH_OTEL_SPANS")
            .output()
            .unwrap();
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            output.status.success(),
            "{test_name} failed in subprocess\n--- stdout\n{stdout}\n--- stderr\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
        // Make sure the subprocess actually ran the test.
        assert!(stdout.contains("1 passed"), "{stdout}");
    }

    #[test]
    fn env_defaults_to_off() {
        in_subprocess("telemetry::tests::env_defaults_to_off", || {
            let env = crate::env::EnvVars::from_env().unwrap();
            assert!(!env.otel_tracing);
            assert_eq!(env.otel_spans, SpanGroups::default());
        });
    }

    #[test]
    fn disabled_is_noop() {
        in_subprocess("telemetry::tests::disabled_is_noop", || {
            assert!(!enabled());
            assert!(!group_enabled(SpanGroup::Block));

            let mut span = Span::root("disabled");
            assert!(!span.is_recording());
            span.set_attr("k", "v");
            assert!(span.context().is_none());
            assert!(Context::current().is_none());

            let mut headers = HeaderMap::new();
            headers.insert("traceparent", HeaderValue::from_static(TRACEPARENT_SAMPLED));
            assert!(extract(&headers).is_none());
        });
    }

    #[test]
    fn span_groups_parse() {
        let groups: SpanGroups = "block, host_fn".parse().unwrap();
        assert!(groups.contains(SpanGroup::Block));
        assert!(groups.contains(SpanGroup::HostFn));
        assert!(!groups.contains(SpanGroup::Store));
        assert_eq!(groups.to_string(), "block,host_fn");

        assert_eq!("all".parse::<SpanGroups>().unwrap(), SpanGroups::ALL);
        assert_eq!("none".parse::<SpanGroups>().unwrap(), SpanGroups::NONE);
        assert_eq!("".parse::<SpanGroups>().unwrap(), SpanGroups::NONE);
        assert_eq!(
            "block,handler,store,graphql".parse::<SpanGroups>().unwrap(),
            SpanGroups::default()
        );
        assert!(!SpanGroups::default().contains(SpanGroup::HostFn));

        let err = "block,hostfn".parse::<SpanGroups>().unwrap_err();
        assert!(err.contains("hostfn"), "{err}");
    }

    #[test]
    fn root_and_child_are_exported() {
        let handle = install();
        assert!(enabled());
        assert!(group_enabled(SpanGroup::HostFn));

        let mut root = Span::root("parent");
        root.set_attr("test", "root_and_child_are_exported");
        root.set_attr("number", 7i64);
        let parent_ctx = root.context().expect("sampled root has a context");
        let mut child = Span::child("child", &parent_ctx);
        child.set_error("boom");
        child.end();
        root.end();

        let roots = handle.spans_with_attr("test", "root_and_child_are_exported");
        assert_eq!(roots.len(), 1);
        let root = &roots[0];
        assert_eq!(root.name, "parent");
        assert_eq!(root.parent_span_id, None);
        assert_eq!(root.attr("number"), Some("7"));

        let children = handle.children_of(root);
        assert_eq!(children.len(), 1);
        assert_eq!(children[0].name, "child");
        assert_eq!(children[0].trace_id, root.trace_id);
        assert!(children[0].is_error);
    }

    #[test]
    fn dropped_span_is_exported() {
        let handle = install();
        {
            let mut span = Span::root("dropped");
            span.set_attr("test", "dropped_span_is_exported");
        }
        assert_eq!(
            handle
                .spans_with_attr("test", "dropped_span_is_exported")
                .len(),
            1
        );
    }

    #[test]
    fn links_are_exported() {
        let handle = install();

        let mut a = Span::root("a");
        a.set_attr("test", "links_are_exported");
        let mut b = Span::root("b");
        b.set_attr("test", "links_are_exported");
        let links = [a.context().unwrap(), b.context().unwrap()];
        let mut linked = Span::root_with_links("linked", &links);
        linked.set_attr("test", "links_are_exported");
        linked.end();
        a.end();
        b.end();

        let spans = handle.spans_with_attr("test", "links_are_exported");
        let linked = spans.iter().find(|s| s.name == "linked").unwrap();
        let mut linked_to: Vec<_> = linked.links.iter().map(|l| l.0.as_str()).collect();
        linked_to.sort();
        let mut expected: Vec<_> = spans
            .iter()
            .filter(|s| s.name != "linked")
            .map(|s| s.trace_id.as_str())
            .collect();
        expected.sort();
        assert_eq!(linked_to, expected);
    }

    #[test]
    fn extract_and_inject_roundtrip() {
        install();

        let mut headers = HeaderMap::new();
        assert!(extract(&headers).is_none());

        headers.insert("traceparent", HeaderValue::from_static(TRACEPARENT_SAMPLED));
        let ctx = extract(&headers).expect("valid traceparent");
        assert_eq!(ctx.trace_id(), "0af7651916cd43dd8448eb211c80319c");

        let child = Span::child("child", &ctx);
        let child_ctx = child.context().unwrap();
        assert_eq!(child_ctx.trace_id(), ctx.trace_id());

        let mut out = HeaderMap::new();
        inject(&child_ctx, &mut out);
        let traceparent = out.get("traceparent").unwrap().to_str().unwrap();
        assert!(
            traceparent.starts_with("00-0af7651916cd43dd8448eb211c80319c-"),
            "{traceparent}"
        );
        assert!(traceparent.ends_with("-01"), "{traceparent}");
    }

    #[test]
    fn unsampled_parent_yields_noop_child() {
        install();

        let mut headers = HeaderMap::new();
        headers.insert(
            "traceparent",
            HeaderValue::from_static(TRACEPARENT_UNSAMPLED),
        );
        let ctx = extract(&headers).expect("an unsampled traceparent is still valid");
        let child = Span::child("child", &ctx);
        assert!(!child.is_recording());
        assert!(child.context().is_none());
    }

    #[test]
    fn attach_makes_context_current() {
        install();

        assert!(Context::current().is_none());
        let span = Span::root("attached");
        let ctx = span.context().unwrap();
        {
            let _guard = ctx.clone().attach();
            let current = Context::current().expect("attached context is current");
            assert_eq!(current.trace_id(), ctx.trace_id());
        }
        assert!(Context::current().is_none());
    }

    #[test]
    fn root_ignores_attached_context() {
        let handle = install();

        let outer = Span::root("outer");
        let outer_ctx = outer.context().unwrap();
        let _guard = outer_ctx.clone().attach();

        let mut root = Span::root("inner");
        root.set_attr("test", "root_ignores_attached_context");
        let root_ctx = root.context().unwrap();
        assert_ne!(root_ctx.trace_id(), outer_ctx.trace_id());
        root.end();

        let spans = handle.spans_with_attr("test", "root_ignores_attached_context");
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].parent_span_id, None);
    }
}
