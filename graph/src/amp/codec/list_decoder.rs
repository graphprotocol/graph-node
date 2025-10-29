use anyhow::Result;

use super::decoder::Decoder;

/// Decodes Arrow lists to vectors of decoded values.
pub(super) struct ListDecoder<'a, T> {
    decoder: T,
    offsets: ArrayOffsets<'a>,
}

/// Contains row index offsets used to determine how many values to decode from an Arrow list.
pub(super) enum ArrayOffsets<'a> {
    Small(&'a [i32]),
    Large(&'a [i64]),
    Fixed(i32),
}

impl<'a, T> ListDecoder<'a, T> {
    /// Creates a new Arrow list decoder with provided `offsets`.
    pub(super) fn new(decoder: T, offsets: ArrayOffsets<'a>) -> Self {
        Self { decoder, offsets }
    }
}

impl<'a, T, V> Decoder<Option<Vec<V>>> for ListDecoder<'a, T>
where
    T: Decoder<V>,
{
    fn decode(&self, row_index: usize) -> Result<Option<Vec<V>>> {
        let Some(range) = self.offsets.range(row_index) else {
            return Ok(None);
        };

        let values = range
            .map(|row_index| self.decoder.decode(row_index))
            .collect::<Result<Vec<_>, _>>()?;

        if values.is_empty() {
            return Ok(None);
        }

        Ok(Some(values))
    }
}

impl<'a> ArrayOffsets<'a> {
    /// Returns row indices belonging to a list at `row_index`.
    fn range(&self, row_index: usize) -> Option<impl Iterator<Item = usize>> {
        match self {
            Self::Small(offsets) => {
                let start = *offsets.get(row_index)? as usize;
                let end = *offsets.get(row_index + 1)? as usize;

                Some(start..end)
            }
            Self::Large(offsets) => {
                let start = *offsets.get(row_index)? as usize;
                let end = *offsets.get(row_index + 1)? as usize;

                Some(start..end)
            }
            Self::Fixed(value_length) => {
                let start = *value_length as usize * row_index;
                let end = *value_length as usize * (row_index + 1);

                Some(start..end)
            }
        }
    }
}

impl<'a> From<&'a [i32]> for ArrayOffsets<'a> {
    fn from(offsets: &'a [i32]) -> Self {
        Self::Small(offsets)
    }
}

impl<'a> From<&'a [i64]> for ArrayOffsets<'a> {
    fn from(offsets: &'a [i64]) -> Self {
        Self::Large(offsets)
    }
}

impl From<i32> for ArrayOffsets<'static> {
    fn from(value_length: i32) -> Self {
        Self::Fixed(value_length)
    }
}
