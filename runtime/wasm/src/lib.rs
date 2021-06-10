pub mod asc_abi;
mod to_from;

/// Public interface of the crate, receives triggers to be processed.
mod host;
pub use host::RuntimeHostBuilder;

/// Pre-processes modules and manages their threads. Serves as an interface from `host` to `module`.
mod mapping;

/// WASM module instance.
mod module;

/// Runtime-agnostic implementation of exports to WASM.
mod host_exports;

mod error;
