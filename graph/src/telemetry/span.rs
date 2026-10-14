use std::borrow::Cow;

use opentelemetry::global::{self, BoxedSpan};
use opentelemetry::trace::{Link, Span as _, SpanBuilder, Status, Tracer};
use opentelemetry::{KeyValue, Value};

use super::{Context, enabled};

/// The instrumentation scope under which all graph-node spans are reported.
const TRACER_NAME: &str = "graph-node";

/// A span attribute value. Mirrors the subset of OpenTelemetry value types
/// that graph-node uses.
#[derive(Clone, Debug, PartialEq)]
pub enum AttrValue {
    Str(Cow<'static, str>),
    I64(i64),
    Bool(bool),
}

impl From<&'static str> for AttrValue {
    fn from(value: &'static str) -> Self {
        AttrValue::Str(Cow::Borrowed(value))
    }
}

impl From<String> for AttrValue {
    fn from(value: String) -> Self {
        AttrValue::Str(Cow::Owned(value))
    }
}

impl From<Cow<'static, str>> for AttrValue {
    fn from(value: Cow<'static, str>) -> Self {
        AttrValue::Str(value)
    }
}

impl From<i64> for AttrValue {
    fn from(value: i64) -> Self {
        AttrValue::I64(value)
    }
}

impl From<i32> for AttrValue {
    fn from(value: i32) -> Self {
        AttrValue::I64(value as i64)
    }
}

impl From<u32> for AttrValue {
    fn from(value: u32) -> Self {
        AttrValue::I64(value as i64)
    }
}

impl From<usize> for AttrValue {
    fn from(value: usize) -> Self {
        AttrValue::I64(i64::try_from(value).unwrap_or(i64::MAX))
    }
}

impl From<bool> for AttrValue {
    fn from(value: bool) -> Self {
        AttrValue::Bool(value)
    }
}

impl From<AttrValue> for Value {
    fn from(value: AttrValue) -> Self {
        match value {
            AttrValue::Str(s) => Value::String(s.into()),
            AttrValue::I64(i) => Value::I64(i),
            AttrValue::Bool(b) => Value::Bool(b),
        }
    }
}

/// A span that is being recorded, or a no-op.
///
/// The span ends when `end` is called or when it is dropped, whichever
/// comes first. A `Span` is a no-op when tracing is disabled or when the
/// trace it belongs to was not sampled; a no-op span has no [`Context`],
/// so nothing can be parented to it and children are never built.
#[must_use = "a span ends when it is dropped"]
pub struct Span(Option<BoxedSpan>);

impl Span {
    /// A span that records nothing.
    pub fn noop() -> Span {
        Span(None)
    }

    /// Start a new trace.
    pub fn root(name: &'static str) -> Span {
        Self::start(name, None, &[])
    }

    /// Start a new trace that points at other traces without being part
    /// of them, for work that was caused by several of them.
    pub fn root_with_links(name: &'static str, links: &[Context]) -> Span {
        Self::start(name, None, links)
    }

    /// Start a span in the trace of `parent`.
    pub fn child(name: &'static str, parent: &Context) -> Span {
        Self::start(name, Some(parent), &[])
    }

    fn start(name: &'static str, parent: Option<&Context>, links: &[Context]) -> Span {
        if !enabled() {
            return Span(None);
        }

        let mut builder = SpanBuilder::from_name(name);
        if !links.is_empty() {
            let links = links
                .iter()
                .map(|ctx| Link::new(ctx.span_context(), Vec::new(), 0))
                .collect();
            builder = builder.with_links(links);
        }

        let tracer = global::tracer(TRACER_NAME);
        let span = match parent {
            Some(parent) => tracer.build_with_context(builder, &parent.0),
            // An explicit empty context, not the thread-local one, so that a
            // root is always a root regardless of what is attached.
            None => tracer.build_with_context(builder, &opentelemetry::Context::new()),
        };

        // Unsampled spans are not exported and must not become parents, so
        // treat them as no-ops from the start.
        if span.span_context().is_sampled() {
            Span(Some(span))
        } else {
            Span(None)
        }
    }

    /// `true` if this span will be exported.
    pub fn is_recording(&self) -> bool {
        self.0.is_some()
    }

    pub fn set_attr(&mut self, key: &'static str, value: impl Into<AttrValue>) {
        if let Some(span) = self.0.as_mut() {
            span.set_attribute(KeyValue::new(key, Value::from(value.into())));
        }
    }

    pub fn set_attrs(&mut self, attrs: impl IntoIterator<Item = (&'static str, AttrValue)>) {
        if let Some(span) = self.0.as_mut() {
            span.set_attributes(
                attrs
                    .into_iter()
                    .map(|(key, value)| KeyValue::new(key, Value::from(value))),
            );
        }
    }

    /// Mark the span as failed. The message shows up as the span status.
    pub fn set_error(&mut self, message: &str) {
        if let Some(span) = self.0.as_mut() {
            span.set_status(Status::error(message.to_owned()));
        }
    }

    /// The context to start children under, or `None` for a no-op span.
    pub fn context(&self) -> Option<Context> {
        self.0
            .as_ref()
            .map(|span| Context::from_span_context(span.span_context().clone()))
    }

    /// End the span now. Dropping it has the same effect; this reads better
    /// when the end is a deliberate point in the code.
    pub fn end(mut self) {
        if let Some(span) = self.0.as_mut() {
            span.end();
        }
    }
}

impl std::fmt::Debug for Span {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.0 {
            Some(span) => write!(f, "Span({})", span.span_context().span_id()),
            None => write!(f, "Span(noop)"),
        }
    }
}
