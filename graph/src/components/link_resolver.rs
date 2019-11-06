use failure;
use serde_json::Value;
use slog::Logger;
use std::time::Duration;
use tokio::prelude::*;

use crate::data::subgraph::Link;

/// The values that `json_stream` returns. The struct contains the deserialized
/// JSON value from the input stream, together with the line number from which
/// the value was read.
pub struct JsonStreamValue {
    pub value: Value,
    pub line: usize,
}

pub type JsonValueStream =
    Box<dyn Stream<Item = JsonStreamValue, Error = failure::Error> + Send + 'static>;

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
    fn cat(
        &self,
        logger: &Logger,
        link: &Link,
    ) -> Box<dyn Future<Item = Vec<u8>, Error = failure::Error> + Send>;

    /// Read the contents of `link` and deserialize them into a stream of JSON
    /// values. The values must each be on a single line; newlines are significant
    /// as they are used to split the file contents and each line is deserialized
    /// separately.
    fn json_stream(
        &self,
        link: &Link,
    ) -> Box<dyn Future<Item = JsonValueStream, Error = failure::Error> + Send + 'static>;
}
