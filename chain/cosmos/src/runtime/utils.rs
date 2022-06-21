use crate::protobuf::*;
use anyhow::anyhow;

/******************************************************************** */
// pub struct AscEvidenceArray(pub  Array<AscPtr<AscEvidence>>);

// impl ToAscObj<AscEvidenceArray> for Vec<Evidence> {
//     fn to_asc_obj<H: AscHeap + ?Sized>(
//         &self,
//         heap: &mut H,
//         gas: &GasCounter,
//     ) -> Result<AscEvidenceArray, DeterministicHostError> {
//         let content: Result<Vec<_>, _> = self.iter().map(|x| asc_new(heap, x, gas)).collect();

//         Ok(AscEvidenceArray(Array::new(&content?, heap, gas)?))
//     }
// }

// impl AscType for AscEvidenceArray {
//     fn to_asc_bytes(&self) -> Result<Vec<u8>, DeterministicHostError> {
//         self.0.to_asc_bytes()
//     }

//     fn from_asc_bytes(
//         asc_obj: &[u8],
//         api_version: &Version,
//     ) -> Result<Self, DeterministicHostError> {
//         Ok(Self(Array::from_asc_bytes(asc_obj, api_version)?))
//     }
// }

impl AscIndexId for AscEvidenceArray {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::CosmosArrayEvidence;
}

//************************************************************** */
//******************************************************* */
// pub struct AscTxResultArray(pub  Array<AscPtr<AscTxResult>>);

// impl ToAscObj<AscTxResultArray> for Vec<codec::TxResult> {
//     fn to_asc_obj<H: AscHeap + ?Sized>(
//         &self,
//         heap: &mut H,
//         gas: &GasCounter,
//     ) -> Result<AscTxResultArray, DeterministicHostError> {
//         let content: Result<Vec<_>, _> = self.iter().map(|x| asc_new(heap, x, gas)).collect();

//         Ok(AscTxResultArray(Array::new(&content?, heap, gas)?))
//     }
// }

// impl AscType for AscTxResultArray {
//     fn to_asc_bytes(&self) -> Result<Vec<u8>, DeterministicHostError> {
//         self.0.to_asc_bytes()
//     }

//     fn from_asc_bytes(
//         asc_obj: &[u8],
//         api_version: &Version,
//     ) -> Result<Self, DeterministicHostError> {
//         Ok(Self(Array::from_asc_bytes(asc_obj, api_version)?))
//     }
// }

impl AscIndexId for AscTxResultArray {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::CosmosArrayTxResult;
}

/* ********************************************************************* */

// pub struct AscValidatorArray(pub  Array<AscPtr<AscValidator>>);

// impl ToAscObj<AscValidatorArray> for Vec<codec::Validator> {
//     fn to_asc_obj<H: AscHeap + ?Sized>(
//         &self,
//         heap: &mut H,
//         gas: &GasCounter,
//     ) -> Result<AscValidatorArray, DeterministicHostError> {
//         let content: Result<Vec<_>, _> = self.iter().map(|x| asc_new(heap, x, gas)).collect();

//         Ok(AscValidatorArray(Array::new(&content?, heap, gas)?))
//     }
// }

// impl AscType for AscValidatorArray {
//     fn to_asc_bytes(&self) -> Result<Vec<u8>, DeterministicHostError> {
//         self.0.to_asc_bytes()
//     }

//     fn from_asc_bytes(
//         asc_obj: &[u8],
//         api_version: &Version,
//     ) -> Result<Self, DeterministicHostError> {
//         Ok(Self(Array::from_asc_bytes(asc_obj, api_version)?))
//     }
// }

impl AscIndexId for AscValidatorArray {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::CosmosArrayValidator;
}

/*************************************************************** */
// pub struct AscCommitSigArray(pub  Array<AscPtr<AscCommitSig>>);

// impl ToAscObj<AscCommitSigArray> for Vec<codec::CommitSig> {
//     fn to_asc_obj<H: AscHeap + ?Sized>(
//         &self,
//         heap: &mut H,
//         gas: &GasCounter,
//     ) -> Result<AscCommitSigArray, DeterministicHostError> {
//         let content: Result<Vec<_>, _> = self.iter().map(|x| asc_new(heap, x, gas)).collect();

//         Ok(AscCommitSigArray(Array::new(&content?, heap, gas)?))
//     }
// }

// impl AscType for AscCommitSigArray {
//     fn to_asc_bytes(&self) -> Result<Vec<u8>, DeterministicHostError> {
//         self.0.to_asc_bytes()
//     }

//     fn from_asc_bytes(
//         asc_obj: &[u8],
//         api_version: &Version,
//     ) -> Result<Self, DeterministicHostError> {
//         Ok(Self(Array::from_asc_bytes(asc_obj, api_version)?))
//     }
// }

impl AscIndexId for AscCommitSigArray {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::CosmosArrayCommitSig;
}

/*************************************************************** */

// pub struct AscEventArray(pub  Array<AscPtr<AscEvent>>);

// impl ToAscObj<AscEventArray> for Vec<codec::Event> {
//     fn to_asc_obj<H: AscHeap + ?Sized>(
//         &self,
//         heap: &mut H,
//         gas: &GasCounter,
//     ) -> Result<AscEventArray, DeterministicHostError> {
//         let content: Result<Vec<_>, _> = self.iter().map(|x| asc_new(heap, x, gas)).collect();

//         Ok(AscEventArray(Array::new(&content?, heap, gas)?))
//     }
// }

// impl AscType for AscEventArray {
//     fn to_asc_bytes(&self) -> Result<Vec<u8>, DeterministicHostError> {
//         self.0.to_asc_bytes()
//     }

//     fn from_asc_bytes(
//         asc_obj: &[u8],
//         api_version: &Version,
//     ) -> Result<Self, DeterministicHostError> {
//         Ok(Self(Array::from_asc_bytes(asc_obj, api_version)?))
//     }
// }

impl AscIndexId for AscEventArray {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::CosmosArrayEvent;
}

/***************************************************************** */
// pub struct AscEventAttributeArray(pub  Array<AscPtr<AscEventAttribute>>);

// impl ToAscObj<AscEventAttributeArray> for Vec<codec::EventAttribute> {
//     fn to_asc_obj<H: AscHeap + ?Sized>(
//         &self,
//         heap: &mut H,
//         gas: &GasCounter,
//     ) -> Result<AscEventAttributeArray, DeterministicHostError> {
//         let content: Result<Vec<_>, _> = self.iter().map(|x| asc_new(heap, x, gas)).collect();

//         Ok(AscEventAttributeArray(Array::new(&content?, heap, gas)?))
//     }
// }

// impl AscType for AscEventAttributeArray {
//     fn to_asc_bytes(&self) -> Result<Vec<u8>, DeterministicHostError> {
//         self.0.to_asc_bytes()
//     }

//     fn from_asc_bytes(
//         asc_obj: &[u8],
//         api_version: &Version,
//     ) -> Result<Self, DeterministicHostError> {
//         Ok(Self(Array::from_asc_bytes(asc_obj, api_version)?))
//     }
// }

impl AscIndexId for AscEventAttributeArray {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::CosmosArrayEventAttribute;
}

/************************************************************************ */
// pub struct AscValidatorUpdateArray(pub  Array<AscPtr<AscValidatorUpdate>>);

// impl ToAscObj<AscValidatorUpdateArray> for Vec<codec::ValidatorUpdate> {
//     fn to_asc_obj<H: AscHeap + ?Sized>(
//         &self,
//         heap: &mut H,
//         gas: &GasCounter,
//     ) -> Result<AscValidatorUpdateArray, DeterministicHostError> {
//         let content: Result<Vec<_>, _> = self.iter().map(|x| asc_new(heap, x, gas)).collect();

//         Ok(AscValidatorUpdateArray(Array::new(&content?, heap, gas)?))
//     }
// }

// impl AscType for AscValidatorUpdateArray {
//     fn to_asc_bytes(&self) -> Result<Vec<u8>, DeterministicHostError> {
//         self.0.to_asc_bytes()
//     }

//     fn from_asc_bytes(
//         asc_obj: &[u8],
//         api_version: &Version,
//     ) -> Result<Self, DeterministicHostError> {
//         Ok(Self(Array::from_asc_bytes(asc_obj, api_version)?))
//     }
// }

impl AscIndexId for AscValidatorUpdateArray {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::CosmosArrayValidatorUpdate;
}

/******************************************************************************* */
pub struct AscBytesArray(pub Array<AscPtr<Uint8Array>>);

impl ToAscObj<AscBytesArray> for Vec<Vec<u8>> {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscBytesArray, DeterministicHostError> {
        let content: Result<Vec<_>, _> =
            self.iter().map(|x| asc_new(heap, &Bytes(x), gas)).collect();

        Ok(AscBytesArray(Array::new(&content?, heap, gas)?))
    }
}

impl AscType for AscBytesArray {
    fn to_asc_bytes(&self) -> Result<Vec<u8>, DeterministicHostError> {
        self.0.to_asc_bytes()
    }

    fn from_asc_bytes(
        asc_obj: &[u8],
        api_version: &Version,
    ) -> Result<Self, DeterministicHostError> {
        Ok(Self(Array::from_asc_bytes(asc_obj, api_version)?))
    }
}

impl AscIndexId for AscBytesArray {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::CosmosArrayBytes;
}

/************************************************************************** */

//there is also another Any in protobuf
impl ToAscObj<AscAny> for prost_types::Any {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscAny, DeterministicHostError> {
        Ok(AscAny {
            type_url: asc_new(heap, &self.type_url, gas)?,
            value: asc_new(heap, &Bytes(&self.value), gas)?,
        })
    }
}

//AscAnyArray is generated for protobuf
// pub struct AscAnyArray(pub  Array<AscPtr<AscAny>>);

impl ToAscObj<AscAnyArray> for Vec<prost_types::Any> {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscAnyArray, DeterministicHostError> {
        let content: Result<Vec<_>, _> = self.iter().map(|x| asc_new(heap, x, gas)).collect();

        Ok(AscAnyArray(Array::new(&content?, heap, gas)?))
    }
}

// impl AscType for AscAnyArray {
//     fn to_asc_bytes(&self) -> Result<Vec<u8>, DeterministicHostError> {
//         self.0.to_asc_bytes()
//     }

//     fn from_asc_bytes(
//         asc_obj: &[u8],
//         api_version: &Version,
//     ) -> Result<Self, DeterministicHostError> {
//         Ok(Self(Array::from_asc_bytes(asc_obj, api_version)?))
//     }
// }

impl AscIndexId for AscAnyArray {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::CosmosArrayAny;
}

/*************************************************************************** */

// pub struct AscSignerInfoArray(pub  Array<AscPtr<AscSignerInfo>>);

// impl ToAscObj<AscSignerInfoArray> for Vec<codec::SignerInfo> {
//     fn to_asc_obj<H: AscHeap + ?Sized>(
//         &self,
//         heap: &mut H,
//         gas: &GasCounter,
//     ) -> Result<AscSignerInfoArray, DeterministicHostError> {
//         let content: Result<Vec<_>, _> = self.iter().map(|x| asc_new(heap, x, gas)).collect();

//         Ok(AscSignerInfoArray(Array::new(&content?, heap, gas)?))
//     }
// }

// impl AscType for AscSignerInfoArray {
//     fn to_asc_bytes(&self) -> Result<Vec<u8>, DeterministicHostError> {
//         self.0.to_asc_bytes()
//     }

//     fn from_asc_bytes(
//         asc_obj: &[u8],
//         api_version: &Version,
//     ) -> Result<Self, DeterministicHostError> {
//         Ok(Self(Array::from_asc_bytes(asc_obj, api_version)?))
//     }
// }

impl AscIndexId for AscSignerInfoArray {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::CosmosArraySignerInfo;
}

/************************************************** */
// pub struct AscModeInfoArray(pub  Array<AscPtr<AscModeInfo>>);

// impl ToAscObj<AscModeInfoArray> for Vec<codec::ModeInfo> {
//     fn to_asc_obj<H: AscHeap + ?Sized>(
//         &self,
//         heap: &mut H,
//         gas: &GasCounter,
//     ) -> Result<AscModeInfoArray, DeterministicHostError> {
//         let content: Result<Vec<_>, _> = self.iter().map(|x| asc_new(heap, x, gas)).collect();

//         Ok(AscModeInfoArray(Array::new(&content?, heap, gas)?))
//     }
// }

// impl AscType for AscModeInfoArray {
//     fn to_asc_bytes(&self) -> Result<Vec<u8>, DeterministicHostError> {
//         self.0.to_asc_bytes()
//     }

//     fn from_asc_bytes(
//         asc_obj: &[u8],
//         api_version: &Version,
//     ) -> Result<Self, DeterministicHostError> {
//         Ok(Self(Array::from_asc_bytes(asc_obj, api_version)?))
//     }
// }

impl AscIndexId for AscModeInfoArray {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::CosmosArrayModeInfo;
}

/**************************************************************************** */

// pub struct AscCoinArray(pub  Array<AscPtr<AscCoin>>);

// impl ToAscObj<AscCoinArray> for Vec<codec::Coin> {
//     fn to_asc_obj<H: AscHeap + ?Sized>(
//         &self,
//         heap: &mut H,
//         gas: &GasCounter,
//     ) -> Result<AscCoinArray, DeterministicHostError> {
//         let content: Result<Vec<_>, _> = self.iter().map(|x| asc_new(heap, x, gas)).collect();

//         Ok(AscCoinArray(Array::new(&content?, heap, gas)?))
//     }
// }

// impl AscType for AscCoinArray {
//     fn to_asc_bytes(&self) -> Result<Vec<u8>, DeterministicHostError> {
//         self.0.to_asc_bytes()
//     }

//     fn from_asc_bytes(
//         asc_obj: &[u8],
//         api_version: &Version,
//     ) -> Result<Self, DeterministicHostError> {
//         Ok(Self(Array::from_asc_bytes(asc_obj, api_version)?))
//     }
// }

impl AscIndexId for AscCoinArray {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::CosmosArrayCoin;
}
/********************************************************************** */

#[repr(C)]
#[derive(AscType)]
pub struct AscPublicKey {
    pub ed25519: AscPtr<Uint8Array>,
    pub secp256k1: AscPtr<Uint8Array>,
}

impl AscIndexId for AscPublicKey {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::CosmosPublicKey;
}

impl ToAscObj<AscPublicKey> for crate::codec::PublicKey {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscPublicKey, DeterministicHostError> {
        use crate::codec::public_key::Sum;

        let sum = self
            .sum
            .as_ref()
            .ok_or_else(|| missing_field_error("PublicKey", "sum"))?;

        let (ed25519, secp256k1) = match sum {
            Sum::Ed25519(e) => (asc_new(heap, &Bytes(e), gas)?, AscPtr::null()),
            Sum::Secp256k1(s) => (AscPtr::null(), asc_new(heap, &Bytes(s), gas)?),
        };

        Ok(AscPublicKey { ed25519, secp256k1 })
    }
}

pub struct Bytes<'a>(pub &'a Vec<u8>);

impl ToAscObj<Uint8Array> for Bytes<'_> {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<Uint8Array, DeterministicHostError> {
        self.0.to_asc_obj(heap, gas)
    }
}
/// Create an error for a missing field in a type.
fn missing_field_error(type_name: &str, field_name: &str) -> DeterministicHostError {
    DeterministicHostError::from(anyhow!("{} missing {}", type_name, field_name))
}

/// Map an optional object to its Asc equivalent if Some, otherwise return null.
pub fn asc_new_or_null<H, O, A>(
    heap: &mut H,
    object: &Option<O>,
    gas: &GasCounter,
) -> Result<AscPtr<A>, DeterministicHostError>
where
    H: AscHeap + ?Sized,
    O: ToAscObj<A>,
    A: AscType + AscIndexId,
{
    match object {
        Some(o) => asc_new(heap, o, gas),
        None => Ok(AscPtr::null()),
    }
}

/// Map an optional object to its Asc equivalent if Some, otherwise return a missing field error.
pub fn asc_new_or_missing<H, O, A>(
    heap: &mut H,
    object: &Option<O>,
    gas: &GasCounter,
    type_name: &str,
    field_name: &str,
) -> Result<AscPtr<A>, DeterministicHostError>
where
    H: AscHeap + ?Sized,
    O: ToAscObj<A>,
    A: AscType + AscIndexId,
{
    match object {
        Some(o) => asc_new(heap, o, gas),
        None => Err(missing_field_error(type_name, field_name)),
    }
}
