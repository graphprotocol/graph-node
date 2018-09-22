use std::collections::HashMap;
use std::hash::Hash;
use std::iter::FromIterator;

use asc_abi::class::*;
use asc_abi::{AscHeap, AscPtr, AscType, AscValue, FromAscObj, ToAscObj};

///! Implementations of `ToAscObj` and `FromAscObj` for Rust types.
///! Standard Rust types go in `mod.rs` and external types in `external.rs`.
mod external;

impl<T: AscValue> ToAscObj<ArrayBuffer<T>> for [T] {
    fn to_asc_obj<H: AscHeap>(&self, _: &H) -> ArrayBuffer<T> {
        ArrayBuffer::new(self)
    }
}

impl<T: AscValue> FromAscObj<ArrayBuffer<T>> for Vec<T> {
    fn from_asc_obj<H: AscHeap>(array_buffer: ArrayBuffer<T>, _: &H) -> Self {
        array_buffer.content.into()
    }
}

impl<T: AscValue> FromAscObj<ArrayBuffer<T>> for [T; 32] {
    fn from_asc_obj<H: AscHeap>(array_buffer: ArrayBuffer<T>, _: &H) -> Self {
        let mut array: [T; 32] = [T::default(); 32];
        array.copy_from_slice(&array_buffer.content);
        array
    }
}

impl<T: AscValue> FromAscObj<ArrayBuffer<T>> for [T; 20] {
    fn from_asc_obj<H: AscHeap>(array_buffer: ArrayBuffer<T>, _: &H) -> Self {
        let mut array: [T; 20] = [T::default(); 20];
        array.copy_from_slice(&array_buffer.content);
        array
    }
}

impl<T: AscValue> FromAscObj<ArrayBuffer<T>> for [T; 4] {
    fn from_asc_obj<H: AscHeap>(array_buffer: ArrayBuffer<T>, _: &H) -> Self {
        let mut array: [T; 4] = [T::default(); 4];
        array.copy_from_slice(&array_buffer.content);
        array
    }
}

impl<T: AscValue> ToAscObj<TypedArray<T>> for [T] {
    fn to_asc_obj<H: AscHeap>(&self, heap: &H) -> TypedArray<T> {
        TypedArray::new(self, heap)
    }
}

impl<T: AscValue> FromAscObj<TypedArray<T>> for Vec<T> {
    fn from_asc_obj<H: AscHeap>(typed_array: TypedArray<T>, heap: &H) -> Self {
        typed_array.get_buffer(heap).content.into()
    }
}

impl<T: AscValue> FromAscObj<TypedArray<T>> for [T; 32] {
    fn from_asc_obj<H: AscHeap>(typed_array: TypedArray<T>, heap: &H) -> Self {
        let mut array: [T; 32] = [T::default(); 32];
        array.copy_from_slice(&typed_array.get_buffer(heap).content);
        array
    }
}

impl<T: AscValue> FromAscObj<TypedArray<T>> for [T; 20] {
    fn from_asc_obj<H: AscHeap>(typed_array: TypedArray<T>, heap: &H) -> Self {
        let mut array: [T; 20] = [T::default(); 20];
        array.copy_from_slice(&typed_array.get_buffer(heap).content);
        array
    }
}

impl<T: AscValue> FromAscObj<TypedArray<T>> for [T; 16] {
    fn from_asc_obj<H: AscHeap>(typed_array: TypedArray<T>, heap: &H) -> Self {
        let mut array: [T; 16] = [T::default(); 16];
        array.copy_from_slice(&typed_array.get_buffer(heap).content);
        array
    }
}

impl<T: AscValue> FromAscObj<TypedArray<T>> for [T; 4] {
    fn from_asc_obj<H: AscHeap>(typed_array: TypedArray<T>, heap: &H) -> Self {
        let mut array: [T; 4] = [T::default(); 4];
        array.copy_from_slice(&typed_array.get_buffer(heap).content);
        array
    }
}

impl ToAscObj<AscString> for str {
    fn to_asc_obj<H: AscHeap>(&self, _: &H) -> AscString {
        AscString::new(&self.encode_utf16().collect::<Vec<_>>())
    }
}

impl ToAscObj<AscString> for String {
    fn to_asc_obj<H: AscHeap>(&self, heap: &H) -> AscString {
        self.as_str().to_asc_obj(heap)
    }
}

impl FromAscObj<AscString> for String {
    fn from_asc_obj<H: AscHeap>(asc_string: AscString, _: &H) -> Self {
        String::from_utf16(&asc_string.content).expect("asc string was not UTF-16")
    }
}

impl<C: AscType, T: ToAscObj<C>> ToAscObj<Array<AscPtr<C>>> for [T] {
    fn to_asc_obj<H: AscHeap>(&self, heap: &H) -> Array<AscPtr<C>> {
        let content: Vec<_> = self.iter().map(|x| heap.asc_new(x)).collect();
        Array::new(&*content, heap)
    }
}

impl<C: AscType, T: FromAscObj<C>> FromAscObj<Array<AscPtr<C>>> for Vec<T> {
    fn from_asc_obj<H: AscHeap>(array: Array<AscPtr<C>>, heap: &H) -> Self {
        array
            .get_buffer(heap)
            .content
            .iter()
            .map(|&x| heap.asc_get(x))
            .collect()
    }
}

impl<K: AscType, V: AscType, T: FromAscObj<K>, U: FromAscObj<V>> FromAscObj<AscTypedMapEntry<K, V>>
    for (T, U)
{
    fn from_asc_obj<H: AscHeap>(asc_entry: AscTypedMapEntry<K, V>, heap: &H) -> Self {
        (heap.asc_get(asc_entry.key), heap.asc_get(asc_entry.value))
    }
}

impl<'a, 'b, K: AscType, V: AscType, T: ToAscObj<K>, U: ToAscObj<V>>
    ToAscObj<AscTypedMapEntry<K, V>> for (&'a T, &'b U)
{
    fn to_asc_obj<H: AscHeap>(&self, heap: &H) -> AscTypedMapEntry<K, V> {
        AscTypedMapEntry {
            key: heap.asc_new(self.0),
            value: heap.asc_new(self.1),
        }
    }
}

impl<K: AscType, V: AscType, T: FromAscObj<K> + Hash + Eq, U: FromAscObj<V>>
    FromAscObj<AscTypedMap<K, V>> for HashMap<T, U>
{
    fn from_asc_obj<H: AscHeap>(asc_map: AscTypedMap<K, V>, heap: &H) -> Self {
        let entries: Vec<(T, U)> = heap.asc_get(asc_map.entries);
        HashMap::from_iter(entries.into_iter())
    }
}
