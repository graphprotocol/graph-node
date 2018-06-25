use super::{AscHeap, AscPtr, AscType, AscValue};
use std::mem::{self, size_of};

///! Rust types that have with a direct correspondence to an Asc class,
///! with their `AscType` implementations.

/// Asc std ArrayBuffer: "a generic, fixed-length raw binary data buffer".
/// See https://github.com/AssemblyScript/assemblyscript/wiki/Memory-Layout-&-Management#arrays
pub struct ArrayBuffer<T> {
    byte_length: u32,
    // Asc allocators always align at 8 bytes, we already have 4 bytes from
    // `byte_length_size` so with 4 more bytes we align the contents at 8
    // bytes. No Asc type has alignment greater than 8, so the
    // elements in `content` will be aligned for any element type.
    _padding: [u8; 4],
    // In Asc this slice is layed out inline with the ArrayBuffer.
    pub content: Box<[T]>,
}

impl<T: AscValue> ArrayBuffer<T> {
    pub(super) fn new(content: &[T]) -> Self {

        // An `AscValue` has the same size in Rust and Asc so:
        let byte_length = size_of::<T>() * content.len();

        assert!(
            byte_length <= u32::max_value() as usize,
            "slice cannot fit in WASM memory"
        );
        let byte_length = byte_length as u32;

        ArrayBuffer {
            byte_length,
            _padding: [0; 4],
            content: content.into(),
        }
    }
}

impl<T: AscValue> AscType for ArrayBuffer<T> {
    fn to_asc_bytes(&self) -> Vec<u8> {
        let mut asc_layout: Vec<u8> = Vec::new();

        // This is just `self.byte_length.to_bytes()` which is unstable.
        let byte_length: [u8; 4] = unsafe { mem::transmute(self.byte_length) };
        asc_layout.extend(&byte_length);
        let padding: [u8; 4] = [0, 0, 0, 0];
        asc_layout.extend(&padding);
        for elem in self.content.iter() {
            asc_layout.extend(elem.to_asc_bytes())
        }
        asc_layout
    }

    /// The Rust representation of an Asc object as layed out in Asc memory.
    fn from_asc_bytes(asc_obj: &[u8]) -> Self {

        // Pointer for our current position within `asc_obj`,
        // initially at the start of the content,
        // skipping `byte_length` and the padding.
        let mut offset = size_of::<u32>() + 4;

        // Read the content.
        let mut content = Vec::new();
        while offset < asc_obj.len() {
            let elem_bytes = &asc_obj[offset..(offset + size_of::<T>())];
            content.push(T::from_asc_bytes(elem_bytes));
            offset += size_of::<T>();
        }

        ArrayBuffer::new(&content)
    }

    /// Calculate the size of the `ArrayBuffer`.
    fn asc_size<H: AscHeap>(ptr: AscPtr<Self>, heap: &H) -> u32 {
        let byte_length = ptr.read_byte_length(heap);
        let byte_length_size = size_of::<u32>() as u32;
        let padding_size = size_of::<u32>() as u32;
        byte_length_size + padding_size + byte_length
    }
}
