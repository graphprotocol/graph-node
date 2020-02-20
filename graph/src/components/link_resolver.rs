use failure;
use futures03::{prelude::Stream, Future};
use serde_json::Value;
use slog::Logger;
use std::pin::Pin;
use std::time::Duration;

use crate::data::subgraph::Link;

/// The values that `json_stream` returns. The struct contains the deserialized
/// JSON value from the input stream, together with the line number from which
/// the value was read.
pub struct JsonStreamValue {
    pub value: Value,
    pub line: usize,
}

pub type JsonValueStream =
    Pin<Box<dyn Stream<Item = Result<JsonStreamValue, failure::Error>> + Send + 'static>>;

/// Resolves links to subgraph manifests and resources referenced by them.
pub trait LinkResolver: Send + Sync + 'static {
    /// Updates the timeout used by the resolver.
    fn with_timeout(self, timeout: Duration) -> Self
    where
        Self: Sized;

    /// Enables infinite retries.
    fn with_retries(self) -> Self
    where
        Self: Sized;

    /// Fetches the link contents as bytes.
    fn cat<'a>(
        &'a self,
        logger: &'a Logger,
        link: &'a Link,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, failure::Error>> + Send + 'a>>;

    /// Read the contents of `link` and deserialize them into a stream of JSON
    /// values. The values must each be on a single line; newlines are significant
    /// as they are used to split the file contents and each line is deserialized
    /// separately.
    fn json_stream<'a>(
        &'a self,
        link: &'a Link,
    ) -> Pin<Box<dyn Future<Output = Result<JsonValueStream, failure::Error>> + Send + 'a>>;
}
