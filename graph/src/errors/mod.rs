use std::fmt::{Display, Error, Formatter};

pub enum ErrorCode {
    SubgraphStartFailure,
    SubgraphExecutionFailure,
    SubgraphExecutionFailureNotRecorded,
}

impl Display for ErrorCode {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        let value = match self {
            SubgraphStartFailure => "SubgraphStartFailure",
            SubgraphStopped => "SubgraphStopped",
            SubgraphExecutionFailure => "SubgraphExecutionFailure",
            SubgraphFailureNotRecorded => "SubgraphExecutionFailureNotRecorded",
        };
        write!(f, "{}", value)
    }
}

impl slog::Value for ErrorCode {
    fn serialize(
        &self,
        _rec: &slog::Record,
        key: slog::Key,
        serializer: &mut slog::Serializer,
    ) -> slog::Result {
        serializer.emit_str(key, format!("{}", self).as_str())
    }
}
