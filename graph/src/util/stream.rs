use std::error::Error;
use std::fmt;

/// Errors that can occur when obtaining an event stream from a system component.
#[derive(Debug)]
pub enum StreamError {
    /// An event stream was previously requested from this component. There can
    /// only be one for each system component at the moment.
    AlreadyCreated,
}

impl Error for StreamError {
    fn description(&self) -> &str {
        "StreamError"
    }

    fn cause(&self) -> Option<&Error> {
        None
    }
}

impl fmt::Display for StreamError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &StreamError::AlreadyCreated => {
                write!(f, "Stream has already been created and can only exist once")
            }
        }
    }
}
