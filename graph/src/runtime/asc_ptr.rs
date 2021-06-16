use super::{get_aligned_length, DeterministicHostError};

use super::{AscHeap, AscIndexId, AscType, IndexForAscTypeId};
use semver::Version;
use std::fmt;
use std::marker::PhantomData;
use std::mem::size_of;

/// A pointer to an object in the Asc heap.
pub struct AscPtr<C>(u32, PhantomData<C>);

impl<T> Copy for AscPtr<T> {}

impl<T> Clone for AscPtr<T> {
    fn clone(&self) -> Self {
        AscPtr(self.0, PhantomData)
    }
}

impl<T> Default for AscPtr<T> {
    fn default() -> Self {
        AscPtr(0, PhantomData)
    }
}

impl<T> fmt::Debug for AscPtr<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl<C> AscPtr<C> {
    /// A raw pointer to be passed to Wasm.
    pub fn wasm_ptr(self) -> u32 {
        self.0
    }

    #[inline(always)]
    pub fn new(heap_ptr: u32) -> Self {
        Self(heap_ptr, PhantomData)
    }
}

impl<C: AscType + AscIndexId> AscPtr<C> {
    /// Create a pointer that is equivalent to AssemblyScript's `null`.
    #[inline(always)]
    pub fn null() -> Self {
        AscPtr::new(0)
    }

    /// Read from `self` into the Rust struct `C`.
    pub fn read_ptr<H: AscHeap + ?Sized>(self, heap: &H) -> Result<C, DeterministicHostError> {
        let len = match heap.api_version() {
            version if version <= Version::new(0, 0, 4) => C::asc_size(self, heap),
            _ => self.read_len(heap),
        }?;
        let bytes = heap.get(self.0, len)?;
        C::from_asc_bytes(&bytes)
    }

    /// Allocate `asc_obj` as an Asc object of class `C`.
    pub fn alloc_obj<H: AscHeap + ?Sized>(
        asc_obj: C,
        heap: &mut H,
    ) -> Result<AscPtr<C>, DeterministicHostError> {
        match heap.api_version() {
            version if version <= Version::new(0, 0, 4) => {
                let heap_ptr = heap.raw_new(&asc_obj.to_asc_bytes()?)?;
                Ok(AscPtr::new(heap_ptr))
            }
            _ => {
                let mut bytes = asc_obj.to_asc_bytes()?;

                let aligned_len = get_aligned_length(bytes.len());
                bytes.extend(std::iter::repeat(0).take(aligned_len));

                let header = Self::generate_header(
                    heap,
                    C::INDEX_ASC_TYPE_ID,
                    asc_obj.content_len(&bytes),
                    bytes.len(),
                )?;
                let header_len = header.len() as u32;

                let heap_ptr = heap.raw_new(&[header, bytes].concat())?;

                // Use header length as offset. so the AscPtr points directly at the content.
                Ok(AscPtr::new(heap_ptr + header_len))
            }
        }
    }

    /// Helper used by arrays and strings to read their length.
    /// Only used for version <= 0.0.4.
    pub fn read_u32<H: AscHeap + ?Sized>(&self, heap: &H) -> Result<u32, DeterministicHostError> {
        // Read the bytes pointed to by `self` as the bytes of a `u32`.
        let raw_bytes = heap.get(self.0, size_of::<u32>() as u32)?;
        let mut u32_bytes: [u8; size_of::<u32>()] = [0; size_of::<u32>()];
        u32_bytes.copy_from_slice(&raw_bytes);
        Ok(u32::from_le_bytes(u32_bytes))
    }

    /// Helper that generates an AssemblyScript (v0.19.2) header.
    /// Only used for version >= 0.0.5.
    fn generate_header<H: AscHeap + ?Sized>(
        heap: &mut H,
        type_id_index: IndexForAscTypeId,
        content_length: usize,
        full_length: usize,
    ) -> Result<Vec<u8>, DeterministicHostError> {
        let mut header: Vec<u8> = Vec::with_capacity(20);

        let gc_info: [u8; 4] = (0u32).to_le_bytes();
        let gc_info2: [u8; 4] = (0u32).to_le_bytes();
        let asc_type_id = heap.asc_type_id(type_id_index)?;
        let rt_id: [u8; 4] = asc_type_id.to_le_bytes();
        let rt_size: [u8; 4] = (content_length as u32).to_le_bytes();

        let mm_info: [u8; 4] =
            ((gc_info.len() + gc_info2.len() + rt_id.len() + rt_size.len() + full_length) as u32)
                .to_le_bytes();

        header.extend(&mm_info);
        header.extend(&gc_info);
        header.extend(&gc_info2);
        header.extend(&rt_id);
        header.extend(&rt_size);

        Ok(header)
    }

    /// Helper to read the length from the header.
    /// An AssemblyScript header is 20 bytes, right before the content, and composed by:
    /// - mm_info: usize
    /// - gc_info: usize
    /// - gc_info2: usize
    /// - rt_id: u32
    /// - rt_size: u32
    /// This function returns the `rt_size`.
    /// Only used for version >= 0.0.5.
    pub fn read_len<H: AscHeap + ?Sized>(&self, heap: &H) -> Result<u32, DeterministicHostError> {
        let size_of_rt_size = 4;
        let start_of_rt_size = self.0.checked_sub(size_of_rt_size).ok_or_else(|| {
            DeterministicHostError(anyhow::anyhow!(
                "Subtract overflow on pointer: {}", // Usually when pointer is zero because of null in AssemblyScript
                self.0
            ))
        })?;
        let raw_bytes = heap.get(start_of_rt_size, size_of::<u32>() as u32)?;
        let mut u32_bytes: [u8; size_of::<u32>()] = [0; size_of::<u32>()];
        u32_bytes.copy_from_slice(&raw_bytes);
        Ok(u32::from_le_bytes(u32_bytes))
    }

    /// Conversion to `u64` for use with `AscEnum`.
    pub fn to_payload(&self) -> u64 {
        self.0 as u64
    }

    /// We typically assume `AscPtr` is never null, but for types such as `string | null` it can be.
    pub fn is_null(&self) -> bool {
        self.0 == 0
    }

    // Erase type information.
    pub fn erase(self) -> AscPtr<()> {
        AscPtr::new(self.0)
    }
}

impl<C> From<u32> for AscPtr<C> {
    fn from(ptr: u32) -> Self {
        AscPtr::new(ptr)
    }
}

impl<T> AscType for AscPtr<T> {
    fn to_asc_bytes(&self) -> Result<Vec<u8>, DeterministicHostError> {
        self.0.to_asc_bytes()
    }

    fn from_asc_bytes(asc_obj: &[u8]) -> Result<Self, DeterministicHostError> {
        let bytes = u32::from_asc_bytes(asc_obj)?;
        Ok(AscPtr::new(bytes))
    }
}
