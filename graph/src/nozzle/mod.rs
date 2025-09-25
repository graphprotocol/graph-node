//! This module contains the functionality required to support Nozzle Subgraphs.

pub mod client;
pub mod error;
pub mod log;

pub use self::client::{flight_client::FlightClient, Client};
