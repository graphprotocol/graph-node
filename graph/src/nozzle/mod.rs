//! This module contains the functionality required to support Nozzle Subgraphs.

pub mod client;
pub mod codec;
pub mod common;
pub mod error;
pub mod log;
pub mod schema;
pub mod sql;
pub mod stream_aggregator;

pub use self::{
    client::{flight_client::FlightClient, Client},
    codec::Codec,
};
