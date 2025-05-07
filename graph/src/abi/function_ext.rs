use std::borrow::Cow;

use alloy::dyn_abi::DynSolType;
use alloy::dyn_abi::DynSolValue;
use alloy::dyn_abi::Specifier;
use alloy::json_abi::Function;
use alloy::json_abi::Param;
use anyhow::anyhow;
use anyhow::Result;
use itertools::Itertools;

use crate::abi::DynSolValueExt;

pub trait FunctionExt {
    /// Returns the signature of this function in the following formats:
    /// - if the function has no outputs: `$name($($inputs),*)`
    /// - if the function has outputs: `$name($($inputs),*):($(outputs),*)`
    ///
    /// Examples:
    /// - `functionName()`
    /// - `functionName():(uint256)`
    /// - `functionName(bool):(uint256,string)`
    /// - `functionName(uint256,bytes32):(string,uint256)`
    fn signature_compat(&self) -> String;

    /// ABI-decodes the given data according to the function's input types.
    fn abi_decode_input(&self, data: &[u8]) -> Result<Vec<DynSolValue>>;

    /// ABI-decodes the given data according to the function's output types.
    fn abi_decode_output(&self, data: &[u8]) -> Result<Vec<DynSolValue>>;

    /// ABI-encodes the given values, prefixed by the function's selector, if any.
    ///
    /// This behaviour is to ensure consistency with `ethabi`.
    fn abi_encode_input(&self, values: &[DynSolValue]) -> Result<Vec<u8>>;
}

impl FunctionExt for Function {
    fn signature_compat(&self) -> String {
        let name = &self.name;
        let inputs = &self.inputs;
        let outputs = &self.outputs;

        // This is what `alloy` uses internally when creating signatures.
        const MAX_SOL_TYPE_LEN: usize = 32;

        let mut sig_cap = name.len() + 1 + inputs.len() * MAX_SOL_TYPE_LEN + 1;

        if !outputs.is_empty() {
            sig_cap = sig_cap + 2 + outputs.len() * MAX_SOL_TYPE_LEN + 1;
        }

        let mut sig = String::with_capacity(sig_cap);

        sig.push_str(&name);
        signature_part(&inputs, &mut sig);

        if !outputs.is_empty() {
            sig.push(':');
            signature_part(&outputs, &mut sig);
        }

        sig
    }

    fn abi_decode_input(&self, data: &[u8]) -> Result<Vec<DynSolValue>> {
        (self as &dyn alloy::dyn_abi::FunctionExt)
            .abi_decode_input(data)
            .map_err(Into::into)
    }

    fn abi_decode_output(&self, data: &[u8]) -> Result<Vec<DynSolValue>> {
        (self as &dyn alloy::dyn_abi::FunctionExt)
            .abi_decode_output(data)
            .map_err(Into::into)
    }

    fn abi_encode_input(&self, values: &[DynSolValue]) -> Result<Vec<u8>> {
        let inputs = &self.inputs;

        if inputs.len() != values.len() {
            return Err(anyhow!(
                "unexpected number of values; expected {}, got {}",
                inputs.len(),
                values.len(),
            ));
        }

        let mut fixed_values = Vec::with_capacity(values.len());

        for (i, input) in inputs.iter().enumerate() {
            let ty = input.resolve()?;
            let val = &values[i];

            fixed_values.push(fix_type_size(&ty, val)?);
        }

        if fixed_values.iter().all(|x| matches!(x, Cow::Borrowed(_))) {
            return (self as &dyn alloy::dyn_abi::JsonAbiExt)
                .abi_encode_input(values)
                .map_err(Into::into);
        }

        // Required because of `alloy::dyn_abi::JsonAbiExt::abi_encode_input` API;
        let owned_fixed_values = fixed_values
            .into_iter()
            .map(|x| x.into_owned())
            .collect_vec();

        (self as &dyn alloy::dyn_abi::JsonAbiExt)
            .abi_encode_input(&owned_fixed_values)
            .map_err(Into::into)
    }
}

// An efficient way to compute a part of the signature without new allocations.
fn signature_part(params: &[Param], out: &mut String) {
    out.push('(');

    match params.len() {
        0 => {}
        1 => {
            params[0].selector_type_raw(out);
        }
        n => {
            params[0].selector_type_raw(out);

            for i in 1..n {
                out.push(',');
                params[i].selector_type_raw(out);
            }
        }
    }

    out.push(')');
}

// Alloy is stricter in type checking than `ehtabi` and requires that the decoded values have
// exactly the same number of bits / bytes as the type used for checking.
//
// This is a problem because in some ASC conversions we lose the original number of bits / bytes
// if the actual data takes less memory.
//
// This method fixes that in a simple but not very cheap way, by encoding the value and trying
// to decode it again using the given type. The result fixes the number of bits / bytes in the
// decoded values, so we can use `alloy` methods that have strict type checking internally.
fn fix_type_size<'a>(ty: &DynSolType, val: &'a DynSolValue) -> Result<Cow<'a, DynSolValue>> {
    if val.matches(ty) {
        return Ok(Cow::Borrowed(val));
    }

    if !val.type_check(ty) {
        return Err(anyhow!(
            "invalid value type; expected '{}', got '{:?}'",
            ty.sol_type_name(),
            val.sol_type_name(),
        ));
    }

    let bytes = val.abi_encode();
    let new_val = ty.abi_decode(&bytes)?;

    Ok(Cow::Owned(new_val))
}

#[cfg(test)]
mod tests {
    use alloy::primitives::I256;
    use alloy::primitives::U256;

    use super::*;

    fn s(f: &str) -> String {
        Function::parse(f).unwrap().signature_compat()
    }

    fn u256(u: u64) -> U256 {
        U256::from(u)
    }

    fn i256(i: i32) -> I256 {
        I256::try_from(i).unwrap()
    }

    #[test]
    fn signature_compat_no_inputs_no_outputs() {
        assert_eq!(s("x()"), "x()");
    }

    #[test]
    fn signature_compat_one_input_no_outputs() {
        assert_eq!(s("x(uint256 a)"), "x(uint256)");
    }

    #[test]
    fn signature_compat_multiple_inputs_no_outputs() {
        assert_eq!(s("x(uint256 a, bytes32 b)"), "x(uint256,bytes32)");
    }

    #[test]
    fn signature_compat_no_inputs_one_output() {
        assert_eq!(s("x() returns (uint256)"), "x():(uint256)");
    }

    #[test]
    fn signature_compat_no_inputs_multiple_outputs() {
        assert_eq!(s("x() returns (uint256, bytes32)"), "x():(uint256,bytes32)");
    }

    #[test]
    fn signature_compat_multiple_inputs_multiple_outputs() {
        assert_eq!(
            s("x(bytes32 a, uint256 b) returns (uint256, bytes32)"),
            "x(bytes32,uint256):(uint256,bytes32)",
        );
    }

    #[test]
    fn abi_decode_input() {
        use DynSolValue::{Int, Tuple, Uint};

        let f = Function::parse("x(uint256 a, int256 b)").unwrap();
        let data = Tuple(vec![Uint(u256(10), 256), Int(i256(-10), 256)]).abi_encode_params();
        let inputs = f.abi_decode_input(&data).unwrap();

        assert_eq!(inputs, vec![Uint(u256(10), 256), Int(i256(-10), 256)]);
    }

    #[test]
    fn abi_decode_output() {
        use DynSolValue::{Int, Tuple, Uint};

        let f = Function::parse("x() returns (uint256 a, int256 b)").unwrap();
        let data = Tuple(vec![Uint(u256(10), 256), Int(i256(-10), 256)]).abi_encode_params();
        let outputs = f.abi_decode_output(&data).unwrap();

        assert_eq!(outputs, vec![Uint(u256(10), 256), Int(i256(-10), 256)]);
    }

    #[test]
    fn abi_encode_input_no_values() {
        let f = Function::parse("x(uint256 a, int256 b)").unwrap();
        let err = f.abi_encode_input(&[]).unwrap_err();

        assert_eq!(
            err.to_string(),
            "unexpected number of values; expected 2, got 0",
        );
    }

    #[test]
    fn abi_encode_input_too_many_values() {
        use DynSolValue::Bool;

        let f = Function::parse("x(uint256 a, int256 b)").unwrap();

        let err = f
            .abi_encode_input(&[Bool(true), Bool(false), Bool(true)])
            .unwrap_err();

        assert_eq!(
            err.to_string(),
            "unexpected number of values; expected 2, got 3",
        );
    }

    #[test]
    fn abi_encode_input_invalid_types() {
        use DynSolValue::Bool;

        let f = Function::parse("x(uint256 a, int256 b)").unwrap();
        let err = f.abi_encode_input(&[Bool(true), Bool(false)]).unwrap_err();
        assert!(err.to_string().starts_with("invalid value type;"));
    }

    #[test]
    fn abi_encode_success() {
        use DynSolValue::{Bool, Uint};

        let f = Function::parse("x(uint256 a, bool b)").unwrap();
        let a = Uint(u256(10), 256);
        let b = Bool(true);

        let data = f.abi_encode_input(&[a.clone(), b.clone()]).unwrap();
        let inputs = f.abi_decode_input(&data[4..]).unwrap();

        assert_eq!(inputs, vec![a, b]);
    }

    #[test]
    fn abi_encode_success_with_size_fix() {
        use DynSolValue::{Int, Uint};

        let f = Function::parse("x(uint256 a, int256 b)").unwrap();
        let a = Uint(u256(10), 32);
        let b = Int(i256(-10), 32);

        let data = f.abi_encode_input(&[a, b]).unwrap();
        let inputs = f.abi_decode_input(&data[4..]).unwrap();

        assert_eq!(inputs, vec![Uint(u256(10), 256), Int(i256(-10), 256)]);
    }
}
