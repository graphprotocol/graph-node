use anyhow::Result;

use super::decoder::Decoder;

/// Decodes Arrow arrays and maps the decoded values to a different type.
pub(super) struct MappingDecoder<T, U, V> {
    decoder: T,
    mapping: Box<dyn Fn(U) -> V + 'static>,
}

impl<T, U, V> MappingDecoder<T, U, V> {
    /// Creates a new decoder that wraps the `decoder`.
    ///
    /// The `mapping` function transforms decoded values from type `U` to type `V`.
    pub(super) fn new(decoder: T, mapping: impl Fn(U) -> V + 'static) -> Self {
        Self {
            decoder,
            mapping: Box::new(mapping),
        }
    }
}

impl<T, U, V> Decoder<V> for MappingDecoder<T, U, V>
where
    T: Decoder<U>,
{
    fn decode(&self, row_index: usize) -> Result<V> {
        let value = self.decoder.decode(row_index)?;

        Ok((&self.mapping)(value))
    }
}
