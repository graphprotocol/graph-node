/// Checks whether errors are deterministic.
pub trait IsDeterministic {
    /// Returns `true` if the error is deterministic.
    fn is_deterministic(&self) -> bool;
}
