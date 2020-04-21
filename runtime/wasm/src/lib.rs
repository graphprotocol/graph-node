use graph::prelude::{Store, SubgraphDeploymentStore};

mod asc_abi;
mod host;
mod to_from;

/// Pre-processes modules and manages their threads. Serves as an interface from `host` to `module`.
mod mapping;

/// Deals with wasmi.
mod module;

/// Runtime-agnostic implementation of exports to WASM.
mod host_exports;

// Public interface of the crate, receives triggers to be processed.

pub use wasmi;

pub trait RuntimeStore: Store + SubgraphDeploymentStore {}
impl<S: Store + SubgraphDeploymentStore> RuntimeStore for S {}

pub use host::{
    HostFunction, HostModule, HostModuleError, HostModules, RuntimeHost, RuntimeHostBuilder,
};
pub use mapping::MappingRequest;
pub use module::WasmiModule;

pub use asc_abi::class::{Array, AscEnum, AscEnumArray, EnumPayload};
pub use asc_abi::{AscHeap, AscPtr, AscType, AscValue, FromAscObj, ToAscObj};
