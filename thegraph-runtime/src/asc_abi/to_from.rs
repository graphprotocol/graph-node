use super::class::*;
use super::{AscHeap, AscPtr, AscType, AscValue, FromAscObj, ToAscObj};
use ethereum_types;

///! Implementations of `ToAscObj` and `FromAscObj` for core Rust types.

impl<T: AscValue> ToAscObj<ArrayBuffer<T>> for [T] {
    fn to_asc_obj<H: AscHeap>(&self, _: &H) -> ArrayBuffer<T> {
        ArrayBuffer::new(self)
    }
}

impl<T: AscValue> FromAscObj<ArrayBuffer<T>> for [T; 20] {
    fn from_asc_obj<H: AscHeap>(array_buffer: ArrayBuffer<T>, _: &H) -> Self {
        assert_eq!(
            array_buffer.content.len(),
            20,
            "wrong ArrayBuffer length, expected 20"
        );
        let mut array: [T; 20] = [T::default(); 20];
        array.copy_from_slice(&array_buffer.content);
        array
    }
}

impl<T: AscValue> FromAscObj<ArrayBuffer<T>> for [T; 4] {
    fn from_asc_obj<H: AscHeap>(array_buffer: ArrayBuffer<T>, _: &H) -> Self {
        assert_eq!(
            array_buffer.content.len(),
            4,
            "wrong ArrayBuffer length, expected 4"
        );
        let mut array: [T; 4] = [T::default(); 4];
        array.copy_from_slice(&array_buffer.content);
        array
    }
}

impl<T: AscValue> FromAscObj<ArrayBuffer<T>> for Vec<T> {
    fn from_asc_obj<H: AscHeap>(array_buffer: ArrayBuffer<T>, _: &H) -> Self {
        array_buffer.content.into()
    }
}

impl ToAscObj<ArrayBuffer<u8>> for ethereum_types::H160 {
    fn to_asc_obj<H: AscHeap>(&self, heap: &H) -> ArrayBuffer<u8> {
        self.0.to_asc_obj(heap)
    }
}

impl FromAscObj<ArrayBuffer<u8>> for ethereum_types::H160 {
    fn from_asc_obj<H: AscHeap>(array_buffer: ArrayBuffer<u8>, heap: &H) -> Self {
        ethereum_types::H160(<[u8; 20]>::from_asc_obj(array_buffer, heap))
    }
}

impl ToAscObj<AscString> for str {
    fn to_asc_obj<H: AscHeap>(&self, _: &H) -> AscString {
        AscString::new(&self.encode_utf16().collect::<Vec<_>>())
    }
}

impl FromAscObj<AscString> for String {
    fn from_asc_obj<H: AscHeap>(asc_string: AscString, _: &H) -> Self {
        String::from_utf16(&asc_string.content).expect("asc string was not UTF-16")
    }
}

impl ToAscObj<ArrayBuffer<u64>> for ethereum_types::U256 {
    fn to_asc_obj<H: AscHeap>(&self, heap: &H) -> ArrayBuffer<u64> {
        self.0.to_asc_obj(heap)
    }
}

impl FromAscObj<ArrayBuffer<u64>> for ethereum_types::U256 {
    fn from_asc_obj<H: AscHeap>(array_buffer: ArrayBuffer<u64>, heap: &H) -> Self {
        ethereum_types::U256(<[u64; 4]>::from_asc_obj(array_buffer, heap))
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
