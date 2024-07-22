/// Used by layers that need to access or extend the dynamic context.
pub trait ExtensibleGraphmanContext {
    /// Adds new data to the context that is available to other layers.
    ///
    /// Requires a custom, unique type for each piece of data.
    fn extend<T>(&mut self, extension: T)
    where
        T: Send + Sync + 'static;

    /// Returns data previously set by other layers.
    fn get<T>(&self) -> Option<&T>
    where
        T: Send + Sync + 'static;
}
