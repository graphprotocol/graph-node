use strum_macros::Display;

#[derive(Display)]
pub enum LogCode {
    SubgraphStartFailure,
    SubgraphSyncingFailure,
    SubgraphSyncingFailureNotRecorded,
    BlockIngestionStatus,
    BlockIngestionLagging,
    GraphQlQuerySuccess,
    GraphQlQueryFailure,
    TokioContention,
}

impl_slog_value!(LogCode, "{}");
