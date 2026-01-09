//! Implementations of `ToAscObj` and `FromAscObj` for Rust types.
//! Standard Rust types go in `mod.rs` and external types in `external.rs`.
use anyhow::anyhow;
use async_trait::async_trait;
use std::collections::HashMap;
use std::hash::Hash;
use std::iter::FromIterator;

use graph::{
    data::value::Word,
    runtime::{
        asc_get, asc_new, gas::GasCounter, AscHeap, AscIndexId, AscPtr, AscType, AscValue,
        DeterministicHostError, FromAscObj, HostExportError, ToAscObj,
    },
};

use crate::asc_abi::class::*;

mod external;

#[async_trait]
impl<T: AscValue + Sync> ToAscObj<TypedArray<T>> for [T] {
    async fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<TypedArray<T>, HostExportError> {
        TypedArray::new(self, heap, gas).await
    }
}

impl<T: AscValue> FromAscObj<TypedArray<T>> for Vec<T> {
    fn from_asc_obj<H: AscHeap + ?Sized>(
        typed_array: TypedArray<T>,

        heap: &H,
        gas: &GasCounter,
        _depth: usize,
    ) -> Result<Self, DeterministicHostError> {
        typed_array.to_vec(heap, gas)
    }
}

impl<T: AscValue + Send + Sync, const LEN: usize> FromAscObj<TypedArray<T>> for [T; LEN] {
    fn from_asc_obj<H: AscHeap + ?Sized>(
        typed_array: TypedArray<T>,

        heap: &H,
        gas: &GasCounter,
        _depth: usize,
    ) -> Result<Self, DeterministicHostError> {
        let v = typed_array.to_vec(heap, gas)?;
        let array = <[T; LEN]>::try_from(v)
            .map_err(|v| anyhow!("expected array of length {}, found length {}", LEN, v.len()))?;
        Ok(array)
    }
}

#[async_trait]
impl ToAscObj<AscString> for str {
    async fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        _gas: &GasCounter,
    ) -> Result<AscString, HostExportError> {
        Ok(AscString::new(
            &self.encode_utf16().collect::<Vec<_>>(),
            heap.api_version(),
        )?)
    }
}

#[async_trait]
impl ToAscObj<AscString> for &str {
    async fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        _gas: &GasCounter,
    ) -> Result<AscString, HostExportError> {
        Ok(AscString::new(
            &self.encode_utf16().collect::<Vec<_>>(),
            heap.api_version(),
        )?)
    }
}

#[async_trait]
impl ToAscObj<AscString> for String {
    async fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscString, HostExportError> {
        self.as_str().to_asc_obj(heap, gas).await
    }
}

#[async_trait]
impl ToAscObj<AscString> for Word {
    async fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscString, HostExportError> {
        self.as_str().to_asc_obj(heap, gas).await
    }
}

impl FromAscObj<AscString> for String {
    fn from_asc_obj<H: AscHeap + ?Sized>(
        asc_string: AscString,
        _: &H,
        _gas: &GasCounter,
        _depth: usize,
    ) -> Result<Self, DeterministicHostError> {
        let mut string = String::from_utf16(asc_string.content())
            .map_err(|e| DeterministicHostError::from(anyhow::Error::from(e)))?;

        // Strip null characters since they are not accepted by Postgres.
        if string.contains('\u{0000}') {
            string = string.replace('\u{0000}', "");
        }
        Ok(string)
    }
}

impl FromAscObj<AscString> for Word {
    fn from_asc_obj<H: AscHeap + ?Sized>(
        asc_string: AscString,

        heap: &H,
        gas: &GasCounter,
        depth: usize,
    ) -> Result<Self, DeterministicHostError> {
        let string = String::from_asc_obj(asc_string, heap, gas, depth)?;

        Ok(Word::from(string))
    }
}

#[async_trait]
impl<C: AscType + AscIndexId + Send + Sync, T: ToAscObj<C> + Sync> ToAscObj<Array<AscPtr<C>>>
    for [T]
{
    async fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<Array<AscPtr<C>>, HostExportError> {
        let mut content = Vec::with_capacity(self.len());
        for x in self {
            content.push(asc_new(heap, x, gas).await?);
        }
        Array::new(&content, heap, gas).await
    }
}

impl<C: AscType + AscIndexId, T: FromAscObj<C>> FromAscObj<Array<AscPtr<C>>> for Vec<T> {
    fn from_asc_obj<H: AscHeap + ?Sized>(
        array: Array<AscPtr<C>>,
        heap: &H,
        gas: &GasCounter,
        depth: usize,
    ) -> Result<Self, DeterministicHostError> {
        array
            .to_vec(heap, gas)?
            .into_iter()
            .map(|x| asc_get(heap, x, gas, depth))
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
        depth: usize,
    ) -> Result<Self, DeterministicHostError> {
        Ok((
            asc_get(heap, asc_entry.key, gas, depth)?,
            asc_get(heap, asc_entry.value, gas, depth)?,
        ))
    }
}

#[async_trait]
impl<K, V, T, U> ToAscObj<AscTypedMapEntry<K, V>> for (T, U)
where
    K: AscType + AscIndexId + Send,
    V: AscType + AscIndexId + Send,
    T: ToAscObj<K> + Sync,
    U: ToAscObj<V> + Sync,
{
    async fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscTypedMapEntry<K, V>, HostExportError> {
        Ok(AscTypedMapEntry {
            key: asc_new(heap, &self.0, gas).await?,
            value: asc_new(heap, &self.1, gas).await?,
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
        depth: usize,
    ) -> Result<Self, DeterministicHostError> {
        let entries: Vec<(T, U)> = asc_get(heap, asc_map.entries, gas, depth)?;
        Ok(HashMap::from_iter(entries))
    }
}
