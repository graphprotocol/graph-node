use super::class::*;
use super::{AscHeap, AscValue, FromAscObj, ToAscObj};
use ethereum_types;

///! Implementations of `ToAscObj` and `FromAscObj` for core Rust types.

impl<T: AscValue> ToAscObj<ArrayBuffer<T>> for [T] {
    fn to_asc_obj<H: AscHeap>(&self, _: &H) -> ArrayBuffer<T> {
        ArrayBuffer::new(self)
    }
}

impl<T: AscValue> FromAscObj<ArrayBuffer<T>> for [T; 20] {
    fn from_asc_obj<H: AscHeap>(array_buffer: &ArrayBuffer<T>, _: &H) -> Self {
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

impl ToAscObj<ArrayBuffer<u8>> for ethereum_types::H160 {
    fn to_asc_obj<H: AscHeap>(&self, heap: &H) -> ArrayBuffer<u8> {
        self.0.to_asc_obj(heap)
    }
}

impl FromAscObj<ArrayBuffer<u8>> for ethereum_types::H160 {
    fn from_asc_obj<H: AscHeap>(array_buffer: &ArrayBuffer<u8>, _: &H) -> Self {
        assert_eq!(
            array_buffer.content.len(),
            20,
            "wrong ArrayBuffer length, expected 20"
        );
        let mut array: [u8; 20] = [0; 20];
        array.copy_from_slice(&array_buffer.content);
        ethereum_types::H160(array)
    }
}

impl ToAscObj<AscString> for str {
    fn to_asc_obj<H: AscHeap>(&self, _: &H) -> AscString {
        AscString::new(&self.encode_utf16().collect::<Vec<_>>())
    }
}

impl FromAscObj<AscString> for String {
    fn from_asc_obj<H: AscHeap>(asc_string: &AscString, _: &H) -> Self {
        String::from_utf16(&asc_string.content).expect("asc string was not UTF-16")
    }
}
