//! An in-memory span exporter for tests.
//!
//! The tracer provider is global to the process, so a test binary installs
//! it once with [`install`] and every test in that binary shares it. Spans
//! are never cleared. Tests tell their own spans apart by putting a unique
//! attribute on them and using [`Handle::spans_with_attr`], or by following
//! a trace id they obtained from a [`Context`].

use std::future::{Future, ready};
use std::process::Command;
use std::sync::{Arc, Mutex, OnceLock};

use opentelemetry::global;
use opentelemetry::trace::{SpanId, Status};
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::{SdkTracerProvider, SpanData, SpanExporter};

use super::{SpanGroups, set_enabled};

/// What the exporter remembers about a span, in plain strings so that tests
/// in other crates do not need the OpenTelemetry types.
#[derive(Clone, Debug, PartialEq)]
pub struct ExportedSpan {
    pub name: String,
    pub trace_id: String,
    pub span_id: String,
    /// `None` for a root span.
    pub parent_span_id: Option<String>,
    pub attributes: Vec<(String, String)>,
    /// `(trace_id, span_id)` of each linked span.
    pub links: Vec<(String, String)>,
    pub is_error: bool,
    /// The status description when `is_error`.
    pub error_message: Option<String>,
}

impl ExportedSpan {
    pub fn attr(&self, key: &str) -> Option<&str> {
        self.attributes
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }

    pub fn has_attr(&self, key: &str, value: &str) -> bool {
        self.attr(key) == Some(value)
    }
}

impl From<&SpanData> for ExportedSpan {
    fn from(data: &SpanData) -> Self {
        let parent_span_id = if data.parent_span_id == SpanId::INVALID {
            None
        } else {
            Some(data.parent_span_id.to_string())
        };
        ExportedSpan {
            name: data.name.to_string(),
            trace_id: data.span_context.trace_id().to_string(),
            span_id: data.span_context.span_id().to_string(),
            parent_span_id,
            attributes: data
                .attributes
                .iter()
                .map(|kv| (kv.key.to_string(), kv.value.to_string()))
                .collect(),
            links: data
                .links
                .iter()
                .map(|link| {
                    (
                        link.span_context.trace_id().to_string(),
                        link.span_context.span_id().to_string(),
                    )
                })
                .collect(),
            is_error: matches!(data.status, Status::Error { .. }),
            error_message: match &data.status {
                Status::Error { description } => Some(description.to_string()),
                _ => None,
            },
        }
    }
}

#[derive(Clone, Debug, Default)]
struct Exporter(Arc<Mutex<Vec<ExportedSpan>>>);

impl SpanExporter for Exporter {
    fn export(&self, batch: Vec<SpanData>) -> impl Future<Output = OTelSdkResult> + Send {
        let mut spans = self.0.lock().unwrap();
        spans.extend(batch.iter().map(ExportedSpan::from));
        ready(Ok(()))
    }
}

/// Access to the spans exported so far in this process.
pub struct Handle {
    exporter: Exporter,
    provider: SdkTracerProvider,
}

impl Handle {
    /// Every span exported so far, in export order.
    pub fn spans(&self) -> Vec<ExportedSpan> {
        // The simple processor exports on span end, so this is a formality.
        let _ = self.provider.force_flush();
        self.exporter.0.lock().unwrap().clone()
    }

    /// Spans carrying `key = value`. Give each test a unique value to pick
    /// out its own spans.
    pub fn spans_with_attr(&self, key: &str, value: &str) -> Vec<ExportedSpan> {
        self.spans()
            .into_iter()
            .filter(|span| span.has_attr(key, value))
            .collect()
    }

    /// Every span of one trace, in export order. Children end before their
    /// parents, so a root is the last span of its trace.
    pub fn trace(&self, trace_id: &str) -> Vec<ExportedSpan> {
        self.spans()
            .into_iter()
            .filter(|span| span.trace_id == trace_id)
            .collect()
    }

    /// Direct children of `parent`, in export order.
    pub fn children_of(&self, parent: &ExportedSpan) -> Vec<ExportedSpan> {
        self.spans()
            .into_iter()
            .filter(|span| span.parent_span_id.as_deref() == Some(parent.span_id.as_str()))
            .collect()
    }
}

static HANDLE: OnceLock<Handle> = OnceLock::new();

/// Install the in-memory exporter as the process-wide tracer provider with
/// every span group enabled, or return the one installed earlier.
pub fn install() -> &'static Handle {
    install_with(SpanGroups::ALL)
}

/// Like [`install`], with only `groups` enabled. The first call in a
/// process decides; later calls return the existing handle whatever their
/// argument, so a test that needs specific groups runs in a subprocess,
/// see [`in_subprocess`].
pub fn install_with(groups: SpanGroups) -> &'static Handle {
    HANDLE.get_or_init(|| {
        let exporter = Exporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        global::set_tracer_provider(provider.clone());
        global::set_text_map_propagator(TraceContextPropagator::new());
        set_enabled(groups);
        Handle { exporter, provider }
    })
}

/// Marks a child process spawned by [`run_in_subprocess`].
pub(super) const CHILD_MARKER: &str = "GRAPH_TELEMETRY_TEST_CHILD";

/// `true` inside a process started by [`run_in_subprocess`].
pub fn in_child_process() -> bool {
    std::env::var_os(CHILD_MARKER).is_some()
}

/// Run `body` in a fresh process with every `OTEL_*` and `GRAPH_OTEL_*`
/// variable unset and `env` set. The tracer provider and the enabled flag
/// are process-global, so this is the only way to observe the disabled
/// state, or to install a provider with specific groups, without racing
/// other tests in the same binary.
///
/// The parent re-executes the test binary for the single test named
/// `test_name` with a marker variable set; the child sees the marker and
/// runs `body` instead of spawning again. The child never mutates its
/// environment, so `body` may start threads freely.
///
/// An `async` test cannot pass its body as a closure; it checks
/// [`in_child_process`] itself and calls [`run_in_subprocess`] when it is
/// the parent.
pub fn in_subprocess(test_name: &str, env: &[(&str, &str)], body: impl FnOnce()) {
    if in_child_process() {
        body();
    } else {
        run_in_subprocess(test_name, env);
    }
}

/// The parent half of [`in_subprocess`]: re-execute this test binary for
/// `test_name` and assert that it passed.
pub fn run_in_subprocess(test_name: &str, env: &[(&str, &str)]) {
    let exe = std::env::current_exe().unwrap();
    let mut command = Command::new(exe);
    command.args(["--exact", test_name, "--test-threads=1"]);
    for (name, _) in std::env::vars() {
        if name.starts_with("OTEL_") || name.starts_with("GRAPH_OTEL_") {
            command.env_remove(name);
        }
    }
    command.env(CHILD_MARKER, "1");
    command.envs(env.iter().copied());

    let output = command.output().unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "{test_name} failed in subprocess\n--- stdout\n{stdout}\n--- stderr\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    // Make sure the subprocess actually ran the test.
    assert!(stdout.contains("1 passed"), "{stdout}");
}
