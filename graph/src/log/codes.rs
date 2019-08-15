use std::fmt::{Display, Error, Formatter};

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

impl Display for LogCode {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        let value = match self {
            LogCode::SubgraphStartFailure => "SubgraphStartFailure",
            LogCode::SubgraphSyncingFailure => "SubgraphSyncingFailure",
            LogCode::SubgraphSyncingFailureNotRecorded => "SubgraphSyncingFailureNotRecorded",
            LogCode::BlockIngestionStatus => "BlockIngestionStatus",
            LogCode::BlockIngestionLagging => "BlockIngestionLagging",
            LogCode::GraphQlQuerySuccess => "GraphQLQuerySuccess",
            LogCode::GraphQlQueryFailure => "GraphQLQueryFailure",
            LogCode::TokioContention => "TokioContention",
        };
        write!(f, "{}", value)
    }
}

impl slog::Value for LogCode {
    fn serialize(
        &self,
        _rec: &slog::Record,
        key: slog::Key,
        serializer: &mut dyn slog::Serializer,
    ) -> slog::Result {
        serializer.emit_str(key, format!("{}", self).as_str())
    }
}
