use std::time::Duration;

/// Contains environment variables related to Amp subgraphs.
#[derive(Debug)]
pub struct AmpEnv {
    /// Maximum number of record batches to buffer in memory per stream for each SQL query.
    /// This is the maximum number of record batches that can be output by a single block.
    ///
    /// Defaults to `1,000`.
    pub max_buffer_size: usize,

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
}

impl AmpEnv {
    const DEFAULT_MAX_BUFFER_SIZE: usize = 1_000;
    const DEFAULT_MAX_BLOCK_RANGE: usize = 2_000_000;
    const DEFAULT_QUERY_RETRY_MIN_DELAY: Duration = Duration::from_secs(1);
    const DEFAULT_QUERY_RETRY_MAX_DELAY: Duration = Duration::from_secs(600);

    pub(super) fn new(raw_env: &super::Inner) -> Self {
        Self {
            max_buffer_size: raw_env
                .amp_max_buffer_size
                .and_then(|value| {
                    if value == 0 {
                        return None;
                    }
                    Some(value)
                })
                .unwrap_or(Self::DEFAULT_MAX_BUFFER_SIZE),
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
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::env::ENV_VARS;

    #[test]
    fn amp_env_constructs_without_flight_fields() {
        // Verify that AmpEnv constructs correctly with only its remaining fields
        // (the Flight service token field has been removed). The ENV_VARS static
        // is constructed at process start; if AmpEnv still had that field, this
        // access would fail to compile.
        let amp = &ENV_VARS.amp;
        assert_eq!(amp.max_buffer_size, AmpEnv::DEFAULT_MAX_BUFFER_SIZE);
        assert_eq!(amp.max_block_range, AmpEnv::DEFAULT_MAX_BLOCK_RANGE);
        assert_eq!(
            amp.query_retry_min_delay,
            AmpEnv::DEFAULT_QUERY_RETRY_MIN_DELAY
        );
        assert_eq!(
            amp.query_retry_max_delay,
            AmpEnv::DEFAULT_QUERY_RETRY_MAX_DELAY
        );
    }
}
