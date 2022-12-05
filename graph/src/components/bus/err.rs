use crate::prelude::thiserror::Error;
use std::fmt::Display;

#[derive(Error, Debug)]
pub enum BusError {
    InitializationError,
    SendMappingError(String),
    SendModificationError(String),
    SendPlainTextError(String),
    SendSchemaMessageError(String),
    BadMessage(String),
    NoRoutingDefinition,
}

impl Display for BusError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BusError::SendMappingError(err) => {
                write!(f, "BusError: sending mapping failed => {:?}", err)
            }
            BusError::SendModificationError(err) => {
                write!(f, "BusError: sending modifications failed => {}", err)
            }
            BusError::SendPlainTextError(err) => {
                write!(f, "BusError: sending plaintext failed => {}", err)
            }
            BusError::SendSchemaMessageError(err) => {
                write!(f, "BusError: sending message with schema failed => {}", err)
            }
            BusError::InitializationError => {
                write!(f, "BusError: initialization failed")
            }
            BusError::BadMessage(err) => {
                write!(f, "BusError: bad format message ({})", err)
            }
            BusError::NoRoutingDefinition => {
                write!(f, "BusError: no routing definition for this")
            }
        }
    }
}
