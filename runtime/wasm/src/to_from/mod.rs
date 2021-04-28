use std::collections::HashMap;
use std::hash::Hash;
use std::iter::FromIterator;

use graph::runtime::{
    AscHeap, AscPtr, AscType, AscValue, DeterministicHostError, FromAscObj, ToAscObj, TryFromAscObj,
};

use crate::asc_abi::class::*;

///! Implementations of `ToAscObj` and `FromAscObj` for Rust types.
///! Standard Rust types go in `mod.rs` and external types in `external.rs`.
mod external;

impl<T: AscValue> ToAscObj<TypedArray<T>> for [T] {
    fn to_asc_obj<H: AscHeap>(
        &self,
        heap: &mut H,
    ) -> Result<TypedArray<T>, DeterministicHostError> {
        TypedArray::new(self, heap)
    }
}

impl<T: AscValue> FromAscObj<TypedArray<T>> for Vec<T> {
    fn from_asc_obj<H: AscHeap>(
        typed_array: TypedArray<T>,
        heap: &H,
    ) -> Result<Self, DeterministicHostError> {
        typed_array.to_vec(heap)
    }
}

impl<T: AscValue> FromAscObj<TypedArray<T>> for [T; 32] {
    fn from_asc_obj<H: AscHeap>(
        typed_array: TypedArray<T>,
        heap: &H,
    ) -> Result<Self, DeterministicHostError> {
        let mut array: [T; 32] = [T::default(); 32];
        let v = typed_array.to_vec(heap)?;
        array.copy_from_slice(&v);
        Ok(array)
    }
}

impl<T: AscValue> FromAscObj<TypedArray<T>> for [T; 20] {
    fn from_asc_obj<H: AscHeap>(
        typed_array: TypedArray<T>,
        heap: &H,
    ) -> Result<Self, DeterministicHostError> {
        let mut array: [T; 20] = [T::default(); 20];
        let v = typed_array.to_vec(heap)?;
        array.copy_from_slice(&v);
        Ok(array)
    }
}

impl<T: AscValue> FromAscObj<TypedArray<T>> for [T; 16] {
    fn from_asc_obj<H: AscHeap>(
        typed_array: TypedArray<T>,
        heap: &H,
    ) -> Result<Self, DeterministicHostError> {
        let mut array: [T; 16] = [T::default(); 16];
        let v = typed_array.to_vec(heap)?;
        array.copy_from_slice(&v);
        Ok(array)
    }
}

impl<T: AscValue> FromAscObj<TypedArray<T>> for [T; 4] {
    fn from_asc_obj<H: AscHeap>(
        typed_array: TypedArray<T>,
        heap: &H,
    ) -> Result<Self, DeterministicHostError> {
        let mut array: [T; 4] = [T::default(); 4];
        let v = typed_array.to_vec(heap)?;
        array.copy_from_slice(&v);
        Ok(array)
    }
}

impl ToAscObj<AscString> for str {
    fn to_asc_obj<H: AscHeap>(&self, _: &mut H) -> Result<AscString, DeterministicHostError> {
        AscString::new(&self.encode_utf16().collect::<Vec<_>>())
    }
}

impl ToAscObj<AscString> for String {
    fn to_asc_obj<H: AscHeap>(&self, heap: &mut H) -> Result<AscString, DeterministicHostError> {
        self.as_str().to_asc_obj(heap)
    }
}

impl FromAscObj<AscString> for String {
    fn from_asc_obj<H: AscHeap>(
        asc_string: AscString,
        _: &H,
    ) -> Result<Self, DeterministicHostError> {
        let mut string = String::from_utf16(&asc_string.content)
            .map_err(|e| DeterministicHostError(e.into()))?;

        // Strip null characters since they are not accepted by Postgres.
        if string.contains("\u{0000}") {
            string = string.replace("\u{0000}", "");
        }
        Ok(string)
    }
}

impl TryFromAscObj<AscString> for String {
    fn try_from_asc_obj<H: AscHeap>(
        asc_string: AscString,
        heap: &H,
    ) -> Result<Self, DeterministicHostError> {
        Ok(Self::from_asc_obj(asc_string, heap)?)
    }
}

impl<C: AscType, T: ToAscObj<C>> ToAscObj<Array<AscPtr<C>>> for [T] {
    fn to_asc_obj<H: AscHeap>(
        &self,
        heap: &mut H,
    ) -> Result<Array<AscPtr<C>>, DeterministicHostError> {
        let content: Result<Vec<_>, _> = self.iter().map(|x| heap.asc_new(x)).collect();
        let content = content?;
        Array::new(&*content, heap)
    }
}

impl<C: AscType, T: FromAscObj<C>> FromAscObj<Array<AscPtr<C>>> for Vec<T> {
    fn from_asc_obj<H: AscHeap>(
        array: Array<AscPtr<C>>,
        heap: &H,
    ) -> Result<Self, DeterministicHostError> {
        array
            .to_vec(heap)?
            .into_iter()
            .map(|x| heap.asc_get(x))
            .collect()
    }
}

impl<C: AscType, T: TryFromAscObj<C>> TryFromAscObj<Array<AscPtr<C>>> for Vec<T> {
    fn try_from_asc_obj<H: AscHeap>(
        array: Array<AscPtr<C>>,
        heap: &H,
    ) -> Result<Self, DeterministicHostError> {
        array
            .to_vec(heap)?
            .into_iter()
            .map(|x| heap.try_asc_get(x))
            .collect()
    }
}

impl<K: AscType, V: AscType, T: TryFromAscObj<K>, U: TryFromAscObj<V>>
    TryFromAscObj<AscTypedMapEntry<K, V>> for (T, U)
{
    fn try_from_asc_obj<H: AscHeap>(
        asc_entry: AscTypedMapEntry<K, V>,
        heap: &H,
    ) -> Result<Self, DeterministicHostError> {
        Ok((
            heap.try_asc_get(asc_entry.key)?,
            heap.try_asc_get(asc_entry.value)?,
        ))
    }
}

impl<K: AscType, V: AscType, T: ToAscObj<K>, U: ToAscObj<V>> ToAscObj<AscTypedMapEntry<K, V>>
    for (T, U)
{
    fn to_asc_obj<H: AscHeap>(
        &self,
        heap: &mut H,
    ) -> Result<AscTypedMapEntry<K, V>, DeterministicHostError> {
        Ok(AscTypedMapEntry {
            key: heap.asc_new(&self.0)?,
            value: heap.asc_new(&self.1)?,
        })
    }
}

impl<K: AscType, V: AscType, T: TryFromAscObj<K> + Hash + Eq, U: TryFromAscObj<V>>
    TryFromAscObj<AscTypedMap<K, V>> for HashMap<T, U>
{
    fn try_from_asc_obj<H: AscHeap>(
        asc_map: AscTypedMap<K, V>,
        heap: &H,
    ) -> Result<Self, DeterministicHostError> {
        let entries: Vec<(T, U)> = heap.try_asc_get(asc_map.entries)?;
        Ok(HashMap::from_iter(entries.into_iter()))
    }
}
