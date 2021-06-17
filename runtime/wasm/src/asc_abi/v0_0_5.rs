use crate::asc_abi::class;
use anyhow::anyhow;
use graph::runtime::{
    get_aligned_length, AscHeap, AscPtr, AscType, AscValue, DeterministicHostError, HEADER_SIZE,
};
use graph_runtime_derive::AscType;
use semver::Version;
use std::marker::PhantomData;
use std::mem::{size_of, size_of_val};

///! Rust types that have with a direct correspondence to an Asc class,
///! with their `AscType` implementations.

/// Asc std ArrayBuffer: "a generic, fixed-length raw binary data buffer".
/// See https://github.com/AssemblyScript/assemblyscript/wiki/Memory-Layout-&-Management#arrays
pub struct ArrayBuffer {
    pub byte_length: u32,
    // In Asc this slice is layed out inline with the ArrayBuffer.
    pub content: Box<[u8]>,
}

impl ArrayBuffer {
    pub fn new<T: AscType>(values: &[T]) -> Result<Self, DeterministicHostError> {
        let mut content = Vec::new();
        for value in values {
            let asc_bytes = value.to_asc_bytes()?;
            // An `AscValue` has size equal to alignment, no padding required.
            content.extend(&asc_bytes);
        }

        if content.len() > u32::max_value() as usize {
            return Err(DeterministicHostError(anyhow::anyhow!(
                "slice cannot fit in WASM memory"
            )));
        }
        Ok(ArrayBuffer {
            byte_length: content.len() as u32,
            content: content.into(),
        })
    }

    /// Read `length` elements of type `T` starting at `byte_offset`.
    ///
    /// Panics if that tries to read beyond the length of `self.content`.
    pub fn get<T: AscType>(
        &self,
        byte_offset: u32,
        length: u32,
        api_version: Version,
    ) -> Result<Vec<T>, DeterministicHostError> {
        let length = length as usize;
        let byte_offset = byte_offset as usize;

        self.content[byte_offset..]
            .chunks(size_of::<T>())
            .take(length)
            .map(|asc_obj| T::from_asc_bytes(asc_obj, api_version.clone()))
            .collect()

        // TODO: This code is preferred as it validates the length of the array.
        // But, some existing subgraphs were found to break when this was added.
        // This needs to be root caused
        /*
        let range = byte_offset..byte_offset + length * size_of::<T>();
        self.content
            .get(range)
            .ok_or_else(|| {
                DeterministicHostError(anyhow::anyhow!("Attempted to read past end of array"))
            })?
            .chunks_exact(size_of::<T>())
            .map(|bytes| T::from_asc_bytes(bytes))
            .collect()
            */
    }
}

impl AscType for ArrayBuffer {
    fn to_asc_bytes(&self) -> Result<Vec<u8>, DeterministicHostError> {
        let mut asc_layout: Vec<u8> = Vec::new();

        asc_layout.extend(self.content.iter());

        // Allocate extra capacity to next power of two, as required by asc.
        let header_size = 20;
        let total_size = self.byte_length as usize + header_size;
        let total_capacity = total_size.next_power_of_two();
        let extra_capacity = total_capacity - total_size;
        asc_layout.extend(std::iter::repeat(0).take(extra_capacity));

        Ok(asc_layout)
    }

    /// The Rust representation of an Asc object as layed out in Asc memory.
    fn from_asc_bytes(
        asc_obj: &[u8],
        _api_version: Version,
    ) -> Result<Self, DeterministicHostError> {
        Ok(ArrayBuffer {
            byte_length: asc_obj.len() as u32,
            content: asc_obj.to_vec().into(),
        })
    }

    fn content_len(&self, _asc_bytes: &[u8]) -> usize {
        self.byte_length as usize // without extra_capacity
    }
}

/// A typed, indexable view of an `ArrayBuffer` of Asc primitives. In Asc it's
/// an abstract class with subclasses for each primitive, for example
/// `Uint8Array` is `TypedArray<u8>`.
///  See https://github.com/AssemblyScript/assemblyscript/wiki/Memory-Layout-&-Management#arrays
#[repr(C)]
#[derive(AscType)]
pub struct TypedArray<T> {
    pub buffer: AscPtr<ArrayBuffer>,
    /// Byte position in `buffer` of the array start.
    data_start: u32,
    byte_length: u32,
    ty: PhantomData<T>,
}

impl<T: AscValue> TypedArray<T> {
    pub(crate) fn new<H: AscHeap + ?Sized>(
        content: &[T],
        heap: &mut H,
    ) -> Result<Self, DeterministicHostError> {
        let buffer = class::ArrayBuffer::new(content, heap.api_version())?;
        let byte_length = content.len() as u32;
        let ptr = AscPtr::alloc_obj(buffer, heap)?;
        Ok(TypedArray {
            buffer: AscPtr::new(ptr.wasm_ptr()), // new AscPtr necessary to convert type parameter
            data_start: ptr.wasm_ptr(),
            byte_length,
            ty: PhantomData,
        })
    }

    pub(crate) fn to_vec<H: AscHeap + ?Sized>(
        &self,
        heap: &H,
    ) -> Result<Vec<T>, DeterministicHostError> {
        self.buffer.read_ptr(heap)?.get(
            self.data_start - self.buffer.wasm_ptr(),
            self.byte_length / size_of::<T>() as u32,
            heap.api_version(),
        )
    }
}

/// Asc std string: "Strings are encoded as UTF-16LE in AssemblyScript, and are
/// prefixed with their length (in character codes) as a 32-bit integer". See
/// https://github.com/AssemblyScript/assemblyscript/wiki/Memory-Layout-&-Management#strings
pub struct AscString {
    // In number of UTF-16 code units (2 bytes each).
    byte_length: u32,
    // The sequence of UTF-16LE code units that form the string.
    pub content: Box<[u16]>,
}

impl AscString {
    pub fn new(content: &[u16]) -> Result<Self, DeterministicHostError> {
        if size_of_val(content) > u32::max_value() as usize {
            return Err(DeterministicHostError(anyhow!(
                "string cannot fit in WASM memory"
            )));
        }

        Ok(AscString {
            byte_length: content.len() as u32,
            content: content.into(),
        })
    }
}

impl AscType for AscString {
    fn to_asc_bytes(&self) -> Result<Vec<u8>, DeterministicHostError> {
        let mut content: Vec<u8> = Vec::new();

        // Write the code points, in little-endian (LE) order.
        for &code_unit in self.content.iter() {
            let low_byte = code_unit as u8;
            let high_byte = (code_unit >> 8) as u8;
            content.push(low_byte);
            content.push(high_byte);
        }

        let header_size = 20;
        let total_size = (self.byte_length as usize * 2) + header_size;
        let total_capacity = total_size.next_power_of_two();
        let extra_capacity = total_capacity - total_size;
        content.extend(std::iter::repeat(0).take(extra_capacity));

        Ok(content)
    }

    /// The Rust representation of an Asc object as layed out in Asc memory.
    fn from_asc_bytes(
        asc_obj: &[u8],
        _api_version: Version,
    ) -> Result<Self, DeterministicHostError> {
        // UTF-16 (used in assemblyscript) always uses one
        // pair of bytes per code unit.
        // https://mathiasbynens.be/notes/javascript-encoding
        // UTF-16 (16-bit Unicode Transformation Format) is an
        // extension of UCS-2 that allows representing code points
        // outside the BMP. It produces a variable-length result
        // of either one or two 16-bit code units per code point.
        // This way, it can encode code points in the range from 0
        // to 0x10FFFF.

        let mut content = Vec::new();
        for pair in asc_obj.chunks(2) {
            let code_point_bytes = [
                pair[0],
                *pair.get(1).ok_or_else(|| {
                    DeterministicHostError(anyhow!(
                        "Attempted to read past end of string content bytes chunk"
                    ))
                })?,
            ];
            let code_point = u16::from_le_bytes(code_point_bytes);
            content.push(code_point);
        }
        AscString::new(&content)
    }

    fn content_len(&self, _asc_bytes: &[u8]) -> usize {
        self.byte_length as usize * 2 // without extra_capacity
    }
}

/// Growable array backed by an `ArrayBuffer`.
/// See https://github.com/AssemblyScript/assemblyscript/wiki/Memory-Layout-&-Management#arrays
#[repr(C)]
#[derive(AscType)]
pub struct Array<T> {
    buffer: AscPtr<ArrayBuffer>,
    buffer_data_start: u32,
    buffer_data_length: u32,
    length: i32,
    ty: PhantomData<T>,
}

impl<T: AscValue> Array<T> {
    pub fn new<H: AscHeap + ?Sized>(
        content: &[T],
        heap: &mut H,
    ) -> Result<Self, DeterministicHostError> {
        let arr_buffer = class::ArrayBuffer::new(content, heap.api_version())?;
        let buffer = AscPtr::alloc_obj(arr_buffer, heap)?;
        let buffer_data_length = buffer.read_len(heap)?;
        Ok(Array {
            buffer: AscPtr::new(buffer.wasm_ptr()),
            buffer_data_start: buffer.wasm_ptr(),
            buffer_data_length,
            length: content.len() as i32,
            ty: PhantomData,
        })
    }

    pub(crate) fn to_vec<H: AscHeap + ?Sized>(
        &self,
        heap: &H,
    ) -> Result<Vec<T>, DeterministicHostError> {
        self.buffer.read_ptr(heap)?.get(
            self.buffer_data_start - self.buffer.wasm_ptr(),
            self.length as u32,
            heap.api_version(),
        )
    }
}
