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
//! The node installs a tracer provider at startup with [`init`], and tests
//! do so with [`testing::install`]. Until one of them runs, [`enabled`] is
//! `false` and every [`Span`] is a no-op.

mod span;
pub mod testing;

use std::fmt;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};

use anyhow::Context as _;
use opentelemetry::trace::{SpanContext, TraceContextExt};
use opentelemetry::{Key, KeyValue, global};
use opentelemetry_http::{HeaderExtractor, HeaderInjector};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::SdkTracerProvider;
use slog::{Logger, info};

use crate::env::EnvVars;

pub use span::{AttrValue, MAX_ERROR_MESSAGE_LEN, Span};

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

/// What [`init`] needs to know, taken from the environment.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Config {
    /// `GRAPH_OTEL_TRACING`
    pub enabled: bool,
    /// `GRAPH_OTEL_SPANS`
    pub groups: SpanGroups,
}

impl From<&EnvVars> for Config {
    fn from(env: &EnvVars) -> Self {
        Config {
            enabled: env.otel_tracing,
            groups: env.otel_spans,
        }
    }
}

/// Keeps the tracer provider alive. Dropping it flushes buffered spans and
/// shuts the exporter down, so it must live until the process is about to
/// exit.
#[must_use = "dropping the guard shuts tracing down"]
pub struct Guard {
    provider: Option<SdkTracerProvider>,
}

impl Guard {
    /// `true` if this guard owns a live tracer provider.
    pub fn is_enabled(&self) -> bool {
        self.provider.is_some()
    }

    /// Export the buffered spans and shut the exporter down. This blocks
    /// until the export finished or timed out, so call it from a blocking
    /// context. Dropping the guard does the same but discards the result.
    pub fn shutdown(mut self) -> Result<(), anyhow::Error> {
        match self.provider.take() {
            Some(provider) => provider
                .shutdown()
                .context("failed to flush spans during shutdown"),
            None => Ok(()),
        }
    }
}

impl Drop for Guard {
    fn drop(&mut self) {
        if let Some(provider) = self.provider.take() {
            // Shutdown drains the batch processor. An error here means
            // spans were lost on the way out, and there is nobody left to
            // tell.
            let _ = provider.shutdown();
        }
    }
}

/// Install the OTLP exporter as the process-wide tracer provider if
/// `config.enabled` is set, and return a guard that keeps it alive. With
/// tracing off, nothing is installed and the guard does nothing.
///
/// The exporter speaks OTLP over `http/protobuf` and is configured through
/// the standard `OTEL_EXPORTER_OTLP_*` environment variables, with
/// `http://localhost:4318` as the default endpoint. The sampler is
/// configured through `OTEL_TRACES_SAMPLER` and `OTEL_TRACES_SAMPLER_ARG`.
/// The service name comes from `OTEL_SERVICE_NAME` and defaults to
/// `graph-node`.
///
/// Must be called at most once per process.
pub fn init(config: &Config, logger: &Logger) -> Result<Guard, anyhow::Error> {
    if !config.enabled {
        return Ok(Guard { provider: None });
    }
    if enabled() {
        anyhow::bail!("OpenTelemetry tracing was already initialized");
    }

    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .build()
        .context("failed to build the OTLP span exporter")?;

    let var = |name: &str| std::env::var(name).ok().filter(|v| !v.is_empty());
    let service_name = service_name(&Resource::builder().build(), var("OTEL_SERVICE_NAME"));
    let resource = Resource::builder()
        .with_service_name(service_name.clone())
        .with_attribute(KeyValue::new("service.version", env!("CARGO_PKG_VERSION")))
        .build();

    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(resource)
        .build();
    global::set_tracer_provider(provider.clone());
    global::set_text_map_propagator(TraceContextPropagator::new());
    set_enabled(config.groups);

    info!(logger, "OpenTelemetry tracing enabled";
          "endpoint" => otlp_traces_endpoint(
              var("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"),
              var("OTEL_EXPORTER_OTLP_ENDPOINT")),
          "service_name" => service_name,
          "spans" => config.groups.to_string());

    Ok(Guard {
        provider: Some(provider),
    })
}

/// The `service.name` to report, in the order the OpenTelemetry
/// specification gives: `OTEL_SERVICE_NAME`, then whatever the SDK
/// detected from `OTEL_RESOURCE_ATTRIBUTES`, then `graph-node`. The SDK's
/// own `unknown_service` fallback is never used, and its detectors do not
/// give `OTEL_SERVICE_NAME` priority on their own.
fn service_name(detected: &Resource, otel_service_name: Option<String>) -> String {
    otel_service_name
        .or_else(|| {
            detected
                .get(&Key::from_static_str("service.name"))
                .map(|value| value.to_string())
                .filter(|name| {
                    !name.is_empty()
                        && name != "unknown_service"
                        && !name.starts_with("unknown_service:")
                })
        })
        .unwrap_or_else(|| "graph-node".to_string())
}

/// The endpoint the exporter will send traces to, for the startup log
/// line. Mirrors the lookup the OTLP crate does internally, given the
/// values of `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` and
/// `OTEL_EXPORTER_OTLP_ENDPOINT`.
fn otlp_traces_endpoint(traces_endpoint: Option<String>, endpoint: Option<String>) -> String {
    traces_endpoint.unwrap_or_else(|| {
        let base = endpoint.unwrap_or_else(|| "http://localhost:4318".to_string());
        format!("{}/v1/traces", base.trim_end_matches('/'))
    })
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
    use http::{HeaderMap, HeaderValue};

    use super::testing::{in_child_process, in_subprocess, install};
    use super::*;

    const TRACEPARENT_SAMPLED: &str = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
    const TRACEPARENT_UNSAMPLED: &str = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-00";

    #[test]
    fn env_defaults_to_off() {
        in_subprocess("telemetry::tests::env_defaults_to_off", &[], || {
            let env = crate::env::EnvVars::from_env().unwrap();
            assert!(!env.otel_tracing);
            assert_eq!(env.otel_spans, SpanGroups::default());
        });
    }

    #[test]
    fn disabled_is_noop() {
        in_subprocess("telemetry::tests::disabled_is_noop", &[], || {
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
    fn config_from_env_defaults_to_off() {
        in_subprocess(
            "telemetry::tests::config_from_env_defaults_to_off",
            &[],
            || {
                let env = crate::env::EnvVars::from_env().unwrap();
                assert_eq!(Config::from(&env), Config::default());
                assert!(!Config::default().enabled);
            },
        );
    }

    #[test]
    fn init_disabled_installs_nothing() {
        let was_enabled = enabled();
        let guard = init(&Config::default(), &crate::log::discard()).unwrap();
        assert!(!guard.is_enabled());
        assert_eq!(enabled(), was_enabled);
        guard.shutdown().unwrap();
        assert_eq!(enabled(), was_enabled);
    }

    #[test]
    fn otlp_endpoint_is_derived_like_the_sdk() {
        let some = |s: &str| Some(s.to_string());
        assert_eq!(
            otlp_traces_endpoint(None, None),
            "http://localhost:4318/v1/traces"
        );
        assert_eq!(
            otlp_traces_endpoint(None, some("http://agent:4318/")),
            "http://agent:4318/v1/traces"
        );
        assert_eq!(
            otlp_traces_endpoint(some("http://x:1/t"), some("http://agent:4318")),
            "http://x:1/t"
        );
    }

    #[test]
    fn service_name_precedence() {
        let detected = |name: &str| {
            Resource::builder_empty()
                .with_service_name(name.to_string())
                .build()
        };
        let none = Resource::builder_empty().build();
        let some = |s: &str| Some(s.to_string());

        assert_eq!(service_name(&none, None), "graph-node");
        assert_eq!(service_name(&detected(""), None), "graph-node");
        assert_eq!(
            service_name(&detected("unknown_service"), None),
            "graph-node"
        );
        assert_eq!(
            service_name(&detected("unknown_service:graph"), None),
            "graph-node"
        );
        assert_eq!(
            service_name(&detected("unknown_service-indexer"), None),
            "unknown_service-indexer"
        );
        assert_eq!(
            service_name(&detected("from-attributes"), None),
            "from-attributes"
        );
        assert_eq!(service_name(&none, some("indexer-7")), "indexer-7");
        assert_eq!(
            service_name(&detected("from-attributes"), some("indexer-7")),
            "indexer-7"
        );
    }

    /// Starts a mock collector in the parent process, then runs `init`
    /// against it in a subprocess. `expected_service_name` must show up in
    /// the exported protobuf body.
    fn export_to_mock_collector(
        test_name: &str,
        env: &[(&str, &str)],
        expected_service_name: &str,
    ) {
        use wiremock::matchers::{header, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        if in_child_process() {
            let config = Config {
                enabled: true,
                groups: SpanGroups::default(),
            };
            let guard = init(&config, &crate::log::discard()).unwrap();
            assert!(guard.is_enabled());
            assert!(enabled());
            assert!(group_enabled(SpanGroup::Block));
            assert!(!group_enabled(SpanGroup::HostFn));

            let mut span = Span::root("exported");
            span.set_attr("test", test_name.to_string());
            span.end();

            // Shutting down flushes the batch processor.
            guard.shutdown().unwrap();
            return;
        }

        let rt = tokio::runtime::Runtime::new().unwrap();
        let server = rt.block_on(async {
            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .and(path("/v1/traces"))
                .and(header("content-type", "application/x-protobuf"))
                .respond_with(ResponseTemplate::new(200))
                .expect(1..)
                .mount(&server)
                .await;
            server
        });

        let uri = server.uri();
        let mut child_env = vec![("OTEL_EXPORTER_OTLP_ENDPOINT", uri.as_str())];
        child_env.extend_from_slice(env);
        in_subprocess(test_name, &child_env, || unreachable!());

        let requests = rt.block_on(async { server.received_requests().await.unwrap() });
        assert!(
            !requests.is_empty(),
            "no export request reached the collector"
        );
        // The resource name and the span name travel as plain strings
        // inside the protobuf body.
        let body = requests
            .iter()
            .flat_map(|req| req.body.iter().copied())
            .collect::<Vec<u8>>();
        let body = String::from_utf8_lossy(&body);
        assert!(
            body.contains(expected_service_name),
            "service.name `{expected_service_name}` missing from export"
        );
        assert!(body.contains("exported"), "span name missing from export");
        rt.block_on(async { drop(server) });
    }

    #[test]
    fn init_exports_over_otlp_http() {
        export_to_mock_collector(
            "telemetry::tests::init_exports_over_otlp_http",
            &[],
            "graph-node",
        );
    }

    #[test]
    fn init_honours_otel_service_name() {
        export_to_mock_collector(
            "telemetry::tests::init_honours_otel_service_name",
            &[
                ("OTEL_SERVICE_NAME", "indexer-7"),
                ("OTEL_RESOURCE_ATTRIBUTES", "service.name=from-attributes"),
            ],
            "indexer-7",
        );
    }

    #[test]
    fn init_honours_resource_attributes_service_name() {
        export_to_mock_collector(
            "telemetry::tests::init_honours_resource_attributes_service_name",
            &[("OTEL_RESOURCE_ATTRIBUTES", "service.name=from-attributes")],
            "from-attributes",
        );
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
    fn error_message_is_bounded() {
        let handle = install();
        let mut span = Span::root("failed");
        span.set_attr("test", "error_message_is_bounded");
        // Multi-byte characters, so that truncation must respect char
        // boundaries to produce valid UTF-8.
        span.set_error(&"ü".repeat(MAX_ERROR_MESSAGE_LEN));
        span.end();

        let spans = handle.spans_with_attr("test", "error_message_is_bounded");
        assert_eq!(spans.len(), 1);
        assert!(spans[0].is_error);
        let message = spans[0].error_message.as_ref().unwrap();
        assert!(message.len() <= MAX_ERROR_MESSAGE_LEN);
        assert!(message.len() > MAX_ERROR_MESSAGE_LEN - 2);
        assert!(message.chars().all(|c| c == 'ü'));
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
