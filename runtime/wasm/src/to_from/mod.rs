use graph::prelude::anyhow::Error;
use std::collections::HashMap;
use std::hash::Hash;
use std::iter::FromIterator;

use crate::asc_abi::class::*;
use crate::asc_abi::{AscHeap, AscPtr, AscType, AscValue, FromAscObj, ToAscObj, TryFromAscObj};

///! Implementations of `ToAscObj` and `FromAscObj` for Rust types.
///! Standard Rust types go in `mod.rs` and external types in `external.rs`.
mod external;

impl<T: AscValue> ToAscObj<TypedArray<T>> for [T] {
    fn to_asc_obj<H: AscHeap>(&self, heap: &mut H) -> TypedArray<T> {
        TypedArray::new(self, heap)
    }
}

impl<T: AscValue> FromAscObj<TypedArray<T>> for Vec<T> {
    fn from_asc_obj<H: AscHeap>(typed_array: TypedArray<T>, heap: &H) -> Self {
        typed_array.to_vec(heap)
    }
}

impl<T: AscValue> FromAscObj<TypedArray<T>> for [T; 32] {
    fn from_asc_obj<H: AscHeap>(typed_array: TypedArray<T>, heap: &H) -> Self {
        let mut array: [T; 32] = [T::default(); 32];
        array.copy_from_slice(&typed_array.to_vec(heap));
        array
    }
}

impl<T: AscValue> FromAscObj<TypedArray<T>> for [T; 20] {
    fn from_asc_obj<H: AscHeap>(typed_array: TypedArray<T>, heap: &H) -> Self {
        let mut array: [T; 20] = [T::default(); 20];
        array.copy_from_slice(&typed_array.to_vec(heap));
        array
    }
}

impl<T: AscValue> FromAscObj<TypedArray<T>> for [T; 16] {
    fn from_asc_obj<H: AscHeap>(typed_array: TypedArray<T>, heap: &H) -> Self {
        let mut array: [T; 16] = [T::default(); 16];
        array.copy_from_slice(&typed_array.to_vec(heap));
        array
    }
}

impl<T: AscValue> FromAscObj<TypedArray<T>> for [T; 4] {
    fn from_asc_obj<H: AscHeap>(typed_array: TypedArray<T>, heap: &H) -> Self {
        let mut array: [T; 4] = [T::default(); 4];
        array.copy_from_slice(&typed_array.to_vec(heap));
        array
    }
}

impl ToAscObj<AscString> for str {
    fn to_asc_obj<H: AscHeap>(&self, _: &mut H) -> AscString {
        AscString::new(&self.encode_utf16().collect::<Vec<_>>())
    }
}

impl ToAscObj<AscString> for String {
    fn to_asc_obj<H: AscHeap>(&self, heap: &mut H) -> AscString {
        self.as_str().to_asc_obj(heap)
    }
}

impl FromAscObj<AscString> for String {
    fn from_asc_obj<H: AscHeap>(asc_string: AscString, _: &H) -> Self {
        let mut string =
            String::from_utf16(&asc_string.content).expect("asc string was not UTF-16");

        // Strip null characters since they are not accepted by Postgres.
        if string.contains("\u{0000}") {
            string = string.replace("\u{0000}", "");
        }
        string
    }
}

impl TryFromAscObj<AscString> for String {
    fn try_from_asc_obj<H: AscHeap>(asc_string: AscString, heap: &H) -> Result<Self, Error> {
        Ok(Self::from_asc_obj(asc_string, heap))
    }
}

impl<C: AscType, T: ToAscObj<C>> ToAscObj<Array<AscPtr<C>>> for [T] {
    fn to_asc_obj<H: AscHeap>(&self, heap: &mut H) -> Array<AscPtr<C>> {
        let content: Vec<_> = self.iter().map(|x| heap.asc_new(x)).collect();
        Array::new(&*content, heap)
    }
}

impl<C: AscType, T: FromAscObj<C>> FromAscObj<Array<AscPtr<C>>> for Vec<T> {
    fn from_asc_obj<H: AscHeap>(array: Array<AscPtr<C>>, heap: &H) -> Self {
        array
            .to_vec(heap)
            .into_iter()
            .map(|x| heap.asc_get(x))
            .collect()
    }
}

impl<C: AscType, T: TryFromAscObj<C>> TryFromAscObj<Array<AscPtr<C>>> for Vec<T> {
    fn try_from_asc_obj<H: AscHeap>(array: Array<AscPtr<C>>, heap: &H) -> Result<Self, Error> {
        array
            .to_vec(heap)
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
    ) -> Result<Self, Error> {
        Ok((
            heap.try_asc_get(asc_entry.key)?,
            heap.try_asc_get(asc_entry.value)?,
        ))
    }
}

impl<'a, 'b, K: AscType, V: AscType, T: ToAscObj<K>, U: ToAscObj<V>>
    ToAscObj<AscTypedMapEntry<K, V>> for (&'a T, &'b U)
{
    fn to_asc_obj<H: AscHeap>(&self, heap: &mut H) -> AscTypedMapEntry<K, V> {
        AscTypedMapEntry {
            key: heap.asc_new(self.0),
            value: heap.asc_new(self.1),
        }
    }
}

impl<K: AscType, V: AscType, T: TryFromAscObj<K> + Hash + Eq, U: TryFromAscObj<V>>
    TryFromAscObj<AscTypedMap<K, V>> for HashMap<T, U>
{
    fn try_from_asc_obj<H: AscHeap>(asc_map: AscTypedMap<K, V>, heap: &H) -> Result<Self, Error> {
        let entries: Vec<(T, U)> = heap.try_asc_get(asc_map.entries)?;
        Ok(HashMap::from_iter(entries.into_iter()))
    }
}
