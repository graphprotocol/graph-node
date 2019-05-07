use std::fmt::{Display, Error, Formatter};

pub enum LogCode {
    SubgraphStartFailure,
    SubgraphSyncingFailure,
    SubgraphSyncingFailureNotRecorded,
    BlockIngestionStatus,
    GraphQlQuerySuccess,
    GraphQlQueryFailure,
}

impl Display for LogCode {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        let value = match self {
            LogCode::SubgraphStartFailure => "SubgraphStartFailure",
            LogCode::SubgraphSyncingFailure => "SubgraphSyncingFailure",
            LogCode::SubgraphSyncingFailureNotRecorded => "SubgraphSyncingFailureNotRecorded",
            LogCode::BlockIngestionStatus => "BlockIngestionStatus",
            LogCode::GraphQlQuerySuccess => "GraphQLQueryTimer",
            LogCode::GraphQlQueryFailure => "GraphQLQueryFailure",
        };
        write!(f, "{}", value)
    }
}

impl slog::Value for LogCode {
    fn serialize(
        &self,
        _rec: &slog::Record,
        key: slog::Key,
        serializer: &mut slog::Serializer,
    ) -> slog::Result {
        serializer.emit_str(key, format!("{}", self).as_str())
    }
}
