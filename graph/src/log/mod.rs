use std::fmt::{Display, Error, Formatter};

pub enum LogCode {
    SubgraphStartFailure,
    SubgraphExecutionFailure,
    SubgraphExecutionFailureNotRecorded,
    BlockSyncStatus,
}

impl Display for LogCode {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        let value = match self {
            SubgraphStartFailure => "SubgraphStartFailure",
            SubgraphStopped => "SubgraphStopped",
            SubgraphExecutionFailure => "SubgraphExecutionFailure",
            SubgraphFailureNotRecorded => "SubgraphExecutionFailureNotRecorded",
            BlockSyncStatus => "BlockSyncStatus",
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
