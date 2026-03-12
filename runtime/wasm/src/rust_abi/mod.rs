//! Rust ABI for WASM subgraph modules.
//!
//! This module provides serialization and host function bindings for
//! Rust-compiled WASM subgraphs, as an alternative to the AssemblyScript
//! ABI in `asc_abi/`.
//!
//! # Protocol
//!
//! - Strings: UTF-8 bytes, passed as (ptr, len)
//! - Entities: TLV (Type-Length-Value) format
//! - Host functions use `graphite` import namespace
//! - Handler signature: `fn(event_ptr: u32, event_len: u32) -> u32`

mod entity;
mod host;
mod trigger;
mod types;

pub use entity::{deserialize_entity_data, serialize_entity, EntityData};
pub use host::{is_rust_module, link_rust_host_functions};
pub use trigger::{RustLogTrigger, ToRustBytes};
pub use types::{FromRustWasm, ToRustWasm, ValueTag};

/// Language enum for dispatch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MappingLanguage {
    AssemblyScript,
    Rust,
}

impl MappingLanguage {
    /// Parse from manifest mapping.kind string.
    pub fn from_kind(kind: &str) -> Option<Self> {
        match kind {
            "wasm/assemblyscript" => Some(Self::AssemblyScript),
            "wasm/rust" => Some(Self::Rust),
            _ => None,
        }
    }
}
