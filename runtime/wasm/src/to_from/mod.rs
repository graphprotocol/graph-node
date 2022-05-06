use std::collections::HashMap;
use std::hash::Hash;
use std::iter::FromIterator;

use graph::runtime::{
    asc_get, asc_new, gas::GasCounter, AscHeap, AscIndexId, AscPtr, AscType, AscValue,
    DeterministicHostError, FromAscObj, ToAscObj,
};

use crate::asc_abi::class::*;

///! Implementations of `ToAscObj` and `FromAscObj` for Rust types.
///! Standard Rust types go in `mod.rs` and external types in `external.rs`.
mod external;

impl<T: AscValue> ToAscObj<TypedArray<T>> for [T] {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<TypedArray<T>, DeterministicHostError> {
        TypedArray::new(self, heap, gas)
    }
}

impl<T: AscValue> FromAscObj<TypedArray<T>> for Vec<T> {
    fn from_asc_obj<H: AscHeap + ?Sized>(
        typed_array: TypedArray<T>,
        heap: &H,
        gas: &GasCounter,
    ) -> Result<Self, DeterministicHostError> {
        typed_array.to_vec(heap, gas)
    }
}

impl<T: AscValue, const LEN: usize> FromAscObj<TypedArray<T>> for [T; LEN] {
    fn from_asc_obj<H: AscHeap + ?Sized>(
        typed_array: TypedArray<T>,
        heap: &H,
        gas: &GasCounter,
    ) -> Result<Self, DeterministicHostError> {
        let mut array: [T; LEN] = [T::default(); LEN];
        let v = typed_array.to_vec(heap, gas)?;
        array.copy_from_slice(&v);
        Ok(array)
    }
}

impl ToAscObj<AscString> for str {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        _gas: &GasCounter,
    ) -> Result<AscString, DeterministicHostError> {
        AscString::new(&self.encode_utf16().collect::<Vec<_>>(), heap.api_version())
    }
}

impl ToAscObj<AscString> for String {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscString, DeterministicHostError> {
        self.as_str().to_asc_obj(heap, gas)
    }
}

impl FromAscObj<AscString> for String {
    fn from_asc_obj<H: AscHeap + ?Sized>(
        asc_string: AscString,
        _: &H,
        _gas: &GasCounter,
    ) -> Result<Self, DeterministicHostError> {
        let mut string = String::from_utf16(asc_string.content())
            .map_err(|e| DeterministicHostError::from(anyhow::Error::from(e)))?;

        // Strip null characters since they are not accepted by Postgres.
        if string.contains('\u{0000}') {
            string = string.replace("\u{0000}", "");
        }
        Ok(string)
    }
}

impl<C: AscType + AscIndexId, T: ToAscObj<C>> ToAscObj<Array<AscPtr<C>>> for [T] {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<Array<AscPtr<C>>, DeterministicHostError> {
        let content: Result<Vec<_>, _> = self.iter().map(|x| asc_new(heap, x, gas)).collect();
        let content = content?;
        Array::new(&*content, heap, gas)
    }
}

impl<C: AscType + AscIndexId, T: FromAscObj<C>> FromAscObj<Array<AscPtr<C>>> for Vec<T> {
    fn from_asc_obj<H: AscHeap + ?Sized>(
        array: Array<AscPtr<C>>,
        heap: &H,
        gas: &GasCounter,
    ) -> Result<Self, DeterministicHostError> {
        array
            .to_vec(heap, gas)?
            .into_iter()
            .map(|x| asc_get(heap, x, gas))
            .collect()
    }
}

impl<K: AscType + AscIndexId, V: AscType + AscIndexId, T: FromAscObj<K>, U: FromAscObj<V>>
    FromAscObj<AscTypedMapEntry<K, V>> for (T, U)
{
    fn from_asc_obj<H: AscHeap + ?Sized>(
        asc_entry: AscTypedMapEntry<K, V>,
        heap: &H,
        gas: &GasCounter,
    ) -> Result<Self, DeterministicHostError> {
        Ok((
            asc_get(heap, asc_entry.key, gas)?,
            asc_get(heap, asc_entry.value, gas)?,
        ))
    }
}

impl<K: AscType + AscIndexId, V: AscType + AscIndexId, T: ToAscObj<K>, U: ToAscObj<V>>
    ToAscObj<AscTypedMapEntry<K, V>> for (T, U)
{
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscTypedMapEntry<K, V>, DeterministicHostError> {
        Ok(AscTypedMapEntry {
            key: asc_new(heap, &self.0, gas)?,
            value: asc_new(heap, &self.1, gas)?,
        })
    }
}

impl<
        K: AscType + AscIndexId,
        V: AscType + AscIndexId,
        T: FromAscObj<K> + Hash + Eq,
        U: FromAscObj<V>,
    > FromAscObj<AscTypedMap<K, V>> for HashMap<T, U>
where
    Array<AscPtr<AscTypedMapEntry<K, V>>>: AscIndexId,
    AscTypedMapEntry<K, V>: AscIndexId,
{
    fn from_asc_obj<H: AscHeap + ?Sized>(
        asc_map: AscTypedMap<K, V>,
        heap: &H,
        gas: &GasCounter,
    ) -> Result<Self, DeterministicHostError> {
        let entries: Vec<(T, U)> = asc_get(heap, asc_map.entries, gas)?;
        Ok(HashMap::from_iter(entries.into_iter()))
    }
}
