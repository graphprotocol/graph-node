//! Facilities for creating and reading objects on the memory of an AssemblyScript (Asc) WASM
//! module. Objects are passed through the `asc_new` and `asc_get` methods of an `AscHeap`
//! implementation. These methods take types that implement `To`/`FromAscObj` and are therefore
//! convertible to/from an `AscType`.

pub mod gas;

mod asc_heap;
mod asc_ptr;

pub use asc_heap::{asc_get, asc_new, try_asc_get, AscHeap, FromAscObj, ToAscObj, TryFromAscObj};
pub use asc_ptr::AscPtr;

use anyhow::Error;
use semver::Version;
use std::convert::TryInto;
use std::fmt;
use std::mem::size_of;

use self::gas::GasCounter;

/// Marker trait for AssemblyScript types that the id should
/// be in the header.
pub trait AscIndexId {
    /// Constant string with the name of the type in AssemblyScript.
    /// This is used to get the identifier for the type in memory layout.
    /// Info about memory layout:
    /// https://www.assemblyscript.org/memory.html#common-header-layout.
    /// Info about identifier (`idof<T>`):
    /// https://www.assemblyscript.org/garbage-collection.html#runtime-interface
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId;
}

/// A type that has a direct correspondence to an Asc type.
///
/// This can be derived for structs that are `#[repr(C)]`, contain no padding
/// and whose fields are all `AscValue`. Enums can derive if they are `#[repr(u32)]`.
///
/// Special classes like `ArrayBuffer` use custom impls.
///
/// See https://github.com/graphprotocol/graph-node/issues/607 for more considerations.
pub trait AscType: Sized {
    /// Transform the Rust representation of this instance into an sequence of
    /// bytes that is precisely the memory layout of a corresponding Asc instance.
    fn to_asc_bytes(&self) -> Result<Vec<u8>, DeterministicHostError>;

    /// The Rust representation of an Asc object as layed out in Asc memory.
    fn from_asc_bytes(
        asc_obj: &[u8],
        api_version: &Version,
    ) -> Result<Self, DeterministicHostError>;

    fn content_len(&self, asc_bytes: &[u8]) -> usize {
        asc_bytes.len()
    }

    /// Size of the corresponding Asc instance in bytes.
    /// Only used for version <= 0.0.3.
    fn asc_size<H: AscHeap + ?Sized>(
        _ptr: AscPtr<Self>,
        _heap: &H,
        _gas: &GasCounter,
    ) -> Result<u32, DeterministicHostError> {
        Ok(std::mem::size_of::<Self>() as u32)
    }
}

// Only implemented because of structs that derive AscType and
// contain fields that are PhantomData.
impl<T> AscType for std::marker::PhantomData<T> {
    fn to_asc_bytes(&self) -> Result<Vec<u8>, DeterministicHostError> {
        Ok(vec![])
    }

    fn from_asc_bytes(
        asc_obj: &[u8],
        _api_version: &Version,
    ) -> Result<Self, DeterministicHostError> {
        assert!(asc_obj.is_empty());

        Ok(Self)
    }
}

/// An Asc primitive or an `AscPtr` into the Asc heap. A type marked as
/// `AscValue` must have the same byte representation in Rust and Asc, including
/// same size, and size must be equal to alignment.
pub trait AscValue: AscType + Copy + Default {}

impl AscType for bool {
    fn to_asc_bytes(&self) -> Result<Vec<u8>, DeterministicHostError> {
        Ok(vec![*self as u8])
    }

    fn from_asc_bytes(
        asc_obj: &[u8],
        _api_version: &Version,
    ) -> Result<Self, DeterministicHostError> {
        if asc_obj.len() != 1 {
            Err(DeterministicHostError::from(anyhow::anyhow!(
                "Incorrect size for bool. Expected 1, got {},",
                asc_obj.len()
            )))
        } else {
            Ok(asc_obj[0] != 0)
        }
    }
}

impl AscValue for bool {}
impl<T> AscValue for AscPtr<T> {}

macro_rules! impl_asc_type {
    ($($T:ty),*) => {
        $(
            impl AscType for $T {
                fn to_asc_bytes(&self) -> Result<Vec<u8>, DeterministicHostError> {
                    Ok(self.to_le_bytes().to_vec())
                }

                fn from_asc_bytes(asc_obj: &[u8], _api_version: &Version) -> Result<Self, DeterministicHostError> {
                    let bytes = asc_obj.try_into().map_err(|_| {
                        DeterministicHostError::from(anyhow::anyhow!(
                            "Incorrect size for {}. Expected {}, got {},",
                            stringify!($T),
                            size_of::<Self>(),
                            asc_obj.len()
                        ))
                    })?;

                    Ok(Self::from_le_bytes(bytes))
                }
            }

            impl AscValue for $T {}
        )*
    };
}

impl_asc_type!(u8, u16, u32, u64, i8, i32, i64, f32, f64);

// The numbers on each variant could just be comments hence the
// `#[repr(u32)]`, however having them in code enforces each value
// to be the same as the docs.
#[repr(u32)]
#[derive(Copy, Clone, Debug)]
pub enum IndexForAscTypeId {
    String = 0,
    ArrayBuffer = 1,
    Int8Array = 2,
    Int16Array = 3,
    Int32Array = 4,
    Int64Array = 5,
    Uint8Array = 6,
    Uint16Array = 7,
    Uint32Array = 8,
    Uint64Array = 9,
    Float32Array = 10,
    Float64Array = 11,
    BigDecimal = 12,
    ArrayBool = 13,
    ArrayUint8Array = 14,
    ArrayEthereumValue = 15,
    ArrayStoreValue = 16,
    ArrayJsonValue = 17,
    ArrayString = 18,
    ArrayEventParam = 19,
    ArrayTypedMapEntryStringJsonValue = 20,
    ArrayTypedMapEntryStringStoreValue = 21,
    SmartContractCall = 22,
    EventParam = 23,
    EthereumTransaction = 24,
    EthereumBlock = 25,
    EthereumCall = 26,
    WrappedTypedMapStringJsonValue = 27,
    WrappedBool = 28,
    WrappedJsonValue = 29,
    EthereumValue = 30,
    StoreValue = 31,
    JsonValue = 32,
    EthereumEvent = 33,
    TypedMapEntryStringStoreValue = 34,
    TypedMapEntryStringJsonValue = 35,
    TypedMapStringStoreValue = 36,
    TypedMapStringJsonValue = 37,
    TypedMapStringTypedMapStringJsonValue = 38,
    ResultTypedMapStringJsonValueBool = 39,
    ResultJsonValueBool = 40,
    ArrayU8 = 41,
    ArrayU16 = 42,
    ArrayU32 = 43,
    ArrayU64 = 44,
    ArrayI8 = 45,
    ArrayI16 = 46,
    ArrayI32 = 47,
    ArrayI64 = 48,
    ArrayF32 = 49,
    ArrayF64 = 50,
    ArrayBigDecimal = 51,

    // Near Type IDs
    //
    // Generated with the following shell script:
    //
    // ```
    // cat chain/near/src/runtime/generated.rs | grep IndexForAscTypeId::Near | grep -Eo "Near[a-zA-Z0-9]+" | awk '{for(x=1;x<=NF;x++)sub(/$/,"="++i+51",")}1' | sed 's/=/ = /'
    // ```
    //
    // The `51` literal at the end in the `awk` should be replaced with the last element
    // value in the list above.
    NearArrayDataReceiver = 52,
    NearArrayCryptoHash = 53,
    NearArrayActionEnum = 54,
    NearArrayMerklePathItem = 55,
    NearArrayValidatorStake = 56,
    NearArraySlashedValidator = 57,
    NearArraySignature = 58,
    NearArrayChunkHeader = 59,
    NearAccessKeyPermissionEnum = 60,
    NearActionEnum = 61,
    NearDirectionEnum = 62,
    NearPublicKey = 63,
    NearSignature = 64,
    NearFunctionCallPermission = 65,
    NearFullAccessPermission = 66,
    NearAccessKey = 67,
    NearDataReceiver = 68,
    NearCreateAccountAction = 69,
    NearDeployContractAction = 70,
    NearFunctionCallAction = 71,
    NearTransferAction = 72,
    NearStakeAction = 73,
    NearAddKeyAction = 74,
    NearDeleteKeyAction = 75,
    NearDeleteAccountAction = 76,
    NearActionReceipt = 77,
    NearSuccessStatusEnum = 78,
    NearMerklePathItem = 79,
    NearExecutionOutcome = 80,
    NearSlashedValidator = 81,
    NearBlockHeader = 82,
    NearValidatorStake = 83,
    NearChunkHeader = 84,
    NearBlock = 85,
    NearReceiptWithOutcome = 86,

    // Tendermint Type IDs
    //
    // Generated with the following shell script:
    //
    // ```
    // cat chain/tendermint/src/runtime/generated.rs | grep IndexForAscTypeId::Tendermint | grep -Eo "Tendermint[a-zA-Z0-9]+" | awk '{for(x=1;x<=NF;x++)sub(/$/,"="++i+86",")}1' | sed 's/=/ = /'
    // ```
    //
    // The `86` literal at the end in the `awk` should be replaced with the last element
    // value in the list above.
    TendermintArrayEventTx = 87,
    TendermintArrayEvent = 88,
    TendermintArrayCommitSig = 89,
    TendermintArrayBytes = 90,
    TendermintArrayEvidence = 91,
    TendermintArrayEventAttribute = 92,
    TendermintBlockIDFlagEnum = 93,
    TendermintSignedMsgTypeEnum = 94,
    TendermintEventList = 95,
    TendermintEventBlock = 96,
    TendermintResponseBeginBlock = 97,
    TendermintResponseEndBlock = 98,
    TendermintValidatorUpdate = 99,
    TendermintArrayValidatorUpdate = 100,
    TendermintConsensusParams = 101,
    TendermintBlockParams = 102,
    TendermintEvidenceParams = 103,
    TendermintValidatorParams = 104,
    TendermintVersionParams = 105,
    TendermintBlock = 106,
    TendermintCommit = 107,
    TendermintCommitSig = 108,
    TendermintHeader = 109,
    TendermintConsensus = 110,
    TendermintBlockID = 111,
    TendermintPartSetHeader = 112,
    TendermintData = 113,
    TendermintEvidence = 114,
    TendermintDuplicateVoteEvidence = 115,
    TendermintEventTx = 116,
    TendermintEventVote = 117,
    TendermintLightClientAttackEvidence = 118,
    TendermintLightBlock = 119,
    TendermintValidatorSet = 120,
    TendermintSignedHeader = 121,
    TendermintEvidenceList = 122,
    TendermintValidator = 123,
    TendermintArrayValidator = 124,
    TendermintPublicKey = 125,
    TendermintTxResult = 126,
    TendermintResponseDeliverTx = 127,
    TendermintEvent = 128,
    TendermintEventAttribute = 129,
    TendermintEventValidatorSetUpdates = 130,
    TendermintDuration = 131,
    TendermintTimestamp = 132,
    TendermintEventData = 133,
}

impl ToAscObj<u32> for IndexForAscTypeId {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        _heap: &mut H,
        _gas: &GasCounter,
    ) -> Result<u32, DeterministicHostError> {
        Ok(*self as u32)
    }
}

#[derive(Debug)]
pub enum DeterministicHostError {
    Gas(Error),
    Other(Error),
}

impl DeterministicHostError {
    pub fn gas(e: Error) -> Self {
        DeterministicHostError::Gas(e)
    }

    pub fn inner(self) -> Error {
        match self {
            DeterministicHostError::Gas(e) | DeterministicHostError::Other(e) => e,
        }
    }
}

impl fmt::Display for DeterministicHostError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DeterministicHostError::Gas(e) | DeterministicHostError::Other(e) => e.fmt(f),
        }
    }
}

impl From<Error> for DeterministicHostError {
    fn from(e: Error) -> DeterministicHostError {
        DeterministicHostError::Other(e)
    }
}

impl std::error::Error for DeterministicHostError {}

#[derive(thiserror::Error, Debug)]
pub enum HostExportError {
    #[error("{0:#}")]
    Unknown(anyhow::Error),

    #[error("{0:#}")]
    PossibleReorg(anyhow::Error),

    #[error("{0:#}")]
    Deterministic(anyhow::Error),
}

impl From<anyhow::Error> for HostExportError {
    fn from(e: anyhow::Error) -> Self {
        HostExportError::Unknown(e)
    }
}

impl From<DeterministicHostError> for HostExportError {
    fn from(value: DeterministicHostError) -> Self {
        match value {
            // Until we are confident on the gas numbers, gas errors are not deterministic
            DeterministicHostError::Gas(e) => HostExportError::Unknown(e),
            DeterministicHostError::Other(e) => HostExportError::Deterministic(e),
        }
    }
}

pub const HEADER_SIZE: usize = 20;

pub fn padding_to_16(content_length: usize) -> usize {
    (16 - (HEADER_SIZE + content_length) % 16) % 16
}
