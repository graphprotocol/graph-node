use crate::{host_exports::HostExportError, module::IntoTrap};
use anyhow::Error;
use graph::components::subgraph::MappingError;
use std::{error, fmt};
use wasmtime::Trap;

#[derive(Debug)]
pub struct DeterministicHostError(pub Error);

pub enum DeterminismLevel {
    /// This error is known to be deterministic. For example, divide by zero.
    /// TODO: For these errors, a further designation should be created about the contents
    /// of the actual message.
    Deterministic,
    /// This error is known to be non-deterministic. For example, an intermittent http failure.
    #[allow(dead_code)]
    NonDeterministic,
    /// An error has not yet been designated as deterministic or not. This should be phased out over time,
    /// and is the default for errors like anyhow which are of an unknown origin.
    Unimplemented,
}

impl fmt::Display for DeterministicHostError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl error::Error for DeterministicHostError {}

impl From<DeterministicHostError> for HostExportError {
    fn from(value: DeterministicHostError) -> Self {
        HostExportError::Deterministic(value.0)
    }
}

impl From<DeterministicHostError> for MappingError {
    fn from(value: DeterministicHostError) -> MappingError {
        MappingError::Unknown(value.0)
    }
}

impl IntoTrap for DeterministicHostError {
    fn determinism_level(&self) -> DeterminismLevel {
        DeterminismLevel::Deterministic
    }
    fn into_trap(self) -> Trap {
        Trap::from(self.0)
    }
}
