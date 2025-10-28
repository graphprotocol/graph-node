use anyhow::Result;

/// Decodes Arrow data at specific row indices into Subgraph types.
///
/// This trait provides a common interface for converting Arrow format data into
/// custom types. Implementations handle the specifics of extracting data from
/// Arrow arrays and constructing the target type `T`.
pub trait Decoder<T> {
    /// Decodes and returns the value at the `row_index`.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The data cannot be converted to type `T`
    /// - The underlying Arrow data is invalid or corrupted
    ///
    /// The returned error is deterministic.
    fn decode(&self, row_index: usize) -> Result<T>;
}

/// Forwards decoding operations through boxed trait objects.
///
/// This implementation enables using `Box<dyn Decoder<T>>` as a decoder,
/// delegating to the underlying implementation.
impl<T> Decoder<T> for Box<dyn Decoder<T> + '_> {
    fn decode(&self, row_index: usize) -> Result<T> {
        (**self).decode(row_index)
    }
}
