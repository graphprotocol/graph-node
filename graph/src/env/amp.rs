use std::time::Duration;

/// Contains environment variables related to Amp subgraphs.
#[derive(Debug)]
pub struct AmpEnv {
    /// Maximum number of record batches to buffer in memory per stream for each SQL query.
    /// This is the maximum number of record batches that can be output by a single block.
    ///
    /// Defaults to `1,000`.
    pub buffer_size: usize,

    /// Maximum number of blocks to request per stream for each SQL query.
    /// Limiting this value reduces load on the Amp server when processing heavy queries.
    ///
    /// Defaults to `2,000,000`.
    pub max_block_range: usize,

    /// Minimum time to wait before retrying a failed SQL query to the Amp server.
    ///
    /// Defaults to `1` second.
    pub query_retry_min_delay: Duration,

    /// Maximum time to wait before retrying a failed SQL query to the Amp server.
    ///
    /// Defaults to `600` seconds.
    pub query_retry_max_delay: Duration,

    /// Token used to authenticate Amp Flight gRPC service requests.
    ///
    /// Defaults to `None`.
    pub flight_service_token: Option<String>,
}

impl AmpEnv {
    const DEFAULT_BUFFER_SIZE: usize = 1_000;
    const DEFAULT_MAX_BLOCK_RANGE: usize = 2_000_000;
    const DEFAULT_QUERY_RETRY_MIN_DELAY: Duration = Duration::from_secs(1);
    const DEFAULT_QUERY_RETRY_MAX_DELAY: Duration = Duration::from_secs(600);

    pub(super) fn new(raw_env: &super::Inner) -> Self {
        Self {
            buffer_size: raw_env
                .amp_buffer_size
                .and_then(|value| {
                    if value == 0 {
                        return None;
                    }
                    Some(value)
                })
                .unwrap_or(Self::DEFAULT_BUFFER_SIZE),
            max_block_range: raw_env
                .amp_max_block_range
                .map(|mut value| {
                    if value == 0 {
                        value = usize::MAX;
                    }
                    value
                })
                .unwrap_or(Self::DEFAULT_MAX_BLOCK_RANGE),
            query_retry_min_delay: raw_env
                .amp_query_retry_min_delay_seconds
                .map(Duration::from_secs)
                .unwrap_or(Self::DEFAULT_QUERY_RETRY_MIN_DELAY),
            query_retry_max_delay: raw_env
                .amp_query_retry_max_delay_seconds
                .map(Duration::from_secs)
                .unwrap_or(Self::DEFAULT_QUERY_RETRY_MAX_DELAY),
            flight_service_token: raw_env.amp_flight_service_token.as_ref().and_then(|value| {
                if value.is_empty() {
                    return None;
                }
                Some(value.to_string())
            }),
        }
    }
}
