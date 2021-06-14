pub mod asc_abi;

mod host;
mod to_from;

/// Public interface of the crate, receives triggers to be processed.

/// Pre-processes modules and manages their threads. Serves as an interface from `host` to `module`.
mod mapping;

/// WASM module instance.
mod module;

/// Runtime-agnostic implementation of exports to WASM.
mod host_exports;

mod error;

pub use host::RuntimeHostBuilder;
pub use host_exports::HostExports;
pub use mapping::{MappingContext, ValidModule};
pub use module::{ExperimentalFeatures, WasmInstance};

#[cfg(debug_assertions)]
pub use module::TRAP_TIMEOUT;
