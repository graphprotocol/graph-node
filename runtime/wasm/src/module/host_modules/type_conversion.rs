use std::str::FromStr;

use lazy_static::lazy_static;
use wasmi::{RuntimeArgs, RuntimeValue};
use web3::types::H160;

use graph::prelude::{format_err, warn, web3, BigInt};

use crate::asc_abi::class::*;
use crate::asc_abi::{AscHeap, AscPtr};
use crate::module::WasmiModule;
use crate::{HostFunction, HostModule, HostModuleError};

use crate::host_module;

pub struct TypeConversionModule {}

impl TypeConversionModule {
    pub fn new() -> Self {
        Self {}
    }
}

host_module! {
    TypeConversionModule,
    TYPE_CONVERSION_MODULE_FUNCS,
    typeConversion,
    {
        bigIntToHex => big_int_to_hex [0],
        bigIntToI32 => big_int_to_i32 [0],
        bigIntToString => big_int_to_string [0],
        bigIntToHex => big_int_to_hex [0],
        bigIntToString => big_int_to_string [0],
        bigIntToI32 => big_int_to_i32 [0],
        bytesToBase58 => bytes_to_base58 [0],
        bytesToHex => bytes_to_hex [0],
        bytesToString => bytes_to_string [0],
        i32ToBigInt => i32_to_big_int [0],
        stringToH160 => string_to_h160 [0],
    }
}

impl TypeConversionModule {
    /// function typeConversion.bigIntToHex(n: Uint8Array): string
    ///
    /// Prints the value of `n` in hex.
    /// Integers are encoded using the least amount of digits (no leading zero digits).
    /// Their encoding may be of uneven length. The number zero encodes as "0x0".
    ///
    /// https://godoc.org/github.com/ethereum/go-ethereum/common/hexutil#hdr-Encoding_Rules
    fn big_int_to_hex(
        &self,
        module: &mut WasmiModule,
        big_int_ptr: AscPtr<AscBigInt>,
    ) -> Result<Option<RuntimeValue>, HostModuleError> {
        let n: BigInt = module.asc_get(big_int_ptr);
        let s = if n == 0.into() {
            "0x0".to_string()
        } else {
            let bytes = n.to_bytes_be().1;
            format!("0x{}", ::hex::encode(bytes).trim_start_matches('0'))
        };
        Ok(Some(RuntimeValue::from(module.asc_new(&s))))
    }

    /// function typeConversion.bigIntToI32(i: Uint64Array): i32
    fn big_int_to_i32(
        &self,
        module: &mut WasmiModule,
        n_ptr: AscPtr<AscBigInt>,
    ) -> Result<Option<RuntimeValue>, HostModuleError> {
        let n: BigInt = module.asc_get(n_ptr);

        if n < i32::min_value().into() || n > i32::max_value().into() {
            return Err(HostModuleError(format_err!(
                "BigInt value does not fit into i32: {}",
                n
            )));
        }

        let n_bytes = n.to_signed_bytes_le();
        let mut i_bytes: [u8; 4] = if n < 0.into() { [255; 4] } else { [0; 4] };
        i_bytes[..n_bytes.len()].copy_from_slice(&n_bytes);
        let i = i32::from_le_bytes(i_bytes);

        Ok(Some(RuntimeValue::from(i)))
    }

    /// function typeConversion.bigIntToString(n: Uint8Array): string
    fn big_int_to_string(
        &self,
        module: &mut WasmiModule,
        big_int_ptr: AscPtr<AscBigInt>,
    ) -> Result<Option<RuntimeValue>, HostModuleError> {
        let bytes: Vec<u8> = module.asc_get(big_int_ptr);
        let n = BigInt::from_signed_bytes_le(&*bytes);
        let s = format!("{}", n);
        Ok(Some(RuntimeValue::from(module.asc_new(&s))))
    }

    /// function typeConversion.bytesToBase58(bytes: Bytes): string
    fn bytes_to_base58(
        &self,
        module: &mut WasmiModule,
        bytes_ptr: AscPtr<Uint8Array>,
    ) -> Result<Option<RuntimeValue>, HostModuleError> {
        let bytes: Vec<u8> = module.asc_get(bytes_ptr);
        let s = ::bs58::encode(&bytes).into_string();
        let result_ptr: AscPtr<AscString> = module.asc_new(&s);
        Ok(Some(RuntimeValue::from(result_ptr)))
    }

    /// Converts bytes to a hex string.
    /// function typeConversion.bytesToHex(bytes: Bytes): string
    ///
    /// References:
    /// https://godoc.org/github.com/ethereum/go-ethereum/common/hexutil#hdr-Encoding_Rules
    /// https://github.com/ethereum/web3.js/blob/f98fe1462625a6c865125fecc9cb6b414f0a5e83/packages/web3-utils/src/utils.js#L283
    fn bytes_to_hex(
        &self,
        module: &mut WasmiModule,
        bytes_ptr: AscPtr<Uint8Array>,
    ) -> Result<Option<RuntimeValue>, HostModuleError> {
        let bytes: Vec<u8> = module.asc_get(bytes_ptr);

        // Even an empty string must be prefixed with `0x`.
        // Encodes each byte as a two hex digits.
        let s = format!("0x{}", ::hex::encode(bytes));

        Ok(Some(RuntimeValue::from(module.asc_new(&s))))
    }

    /// function typeConversion.bytesToString(bytes: Bytes): string
    fn bytes_to_string(
        &self,
        module: &mut WasmiModule,
        bytes_ptr: AscPtr<Uint8Array>,
    ) -> Result<Option<RuntimeValue>, HostModuleError> {
        let bytes: Vec<u8> = module.asc_get(bytes_ptr);
        let s = String::from_utf8_lossy(&bytes);

        // If the string was re-allocated, that means it was not UTF-8.
        if matches!(s, std::borrow::Cow::Owned(_)) {
            warn!(
                module.ctx.logger,
                "Bytes contain invalid UTF8. This may be caused by attempting \
                     to convert a value such as an address that cannot be parsed to \
                     a unicode string. You may want to use 'toHexString()' instead. \
                     String: '{}'",
                s,
            )
        }
        // The string may have been encoded in a fixed length buffer and padded
        // with null characters, so trim trailing nulls.
        let string = s.trim_end_matches('\u{0000}').to_string();

        Ok(Some(RuntimeValue::from(module.asc_new(&string))))
    }

    /// function typeConversion.i32ToBigInt(i: i32): Uint64Array
    fn i32_to_big_int(
        &self,
        module: &mut WasmiModule,
        i: i32,
    ) -> Result<Option<RuntimeValue>, HostModuleError> {
        let bytes = BigInt::from(i).to_signed_bytes_le();
        Ok(Some(RuntimeValue::from(module.asc_new(&*bytes))))
    }

    /// function typeConversion.stringToH160(s: String): H160
    fn string_to_h160(
        &self,
        module: &mut WasmiModule,
        s_ptr: AscPtr<AscString>,
    ) -> Result<Option<RuntimeValue>, HostModuleError> {
        let s: String = module.asc_get(s_ptr);

        // `H160::from_str` takes a hex string with no leading `0x`.
        let string = s.trim_start_matches("0x");
        let h160 = H160::from_str(string).map_err(|e| {
            HostModuleError(format_err!(
                "Failed to convert string to Address/H160: {}",
                e
            ))
        })?;

        let h160_obj: AscPtr<AscH160> = module.asc_new(&h160);
        Ok(Some(RuntimeValue::from(h160_obj)))
    }
}
