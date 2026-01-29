//! JSON-RPC admin server for subgraph management.
//!
//! This crate provides a JSON-RPC 2.0 server for managing subgraphs,
//! supporting operations like create, deploy, remove, reassign, pause, and resume.

mod handlers;
mod jsonrpc;
mod server;

pub use server::{JsonRpcServer, JsonRpcServerError};
