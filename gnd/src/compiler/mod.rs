//! Compiler module for building subgraph mappings.
//!
//! This module provides functionality to compile AssemblyScript mappings
//! to WebAssembly using the AssemblyScript compiler (asc).

mod asc;

pub use asc::{compile_mapping, find_graph_ts, AscCompileOptions};
