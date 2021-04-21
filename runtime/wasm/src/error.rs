use crate::{host_exports::HostExportError, module::IntoTrap};
use graph::runtime::DeterministicHostError;
use wasmtime::Trap;

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

impl From<DeterministicHostError> for HostExportError {
    fn from(value: DeterministicHostError) -> Self {
        HostExportError::Deterministic(value.0)
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
