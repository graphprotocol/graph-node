use alloy::dyn_abi::DynSolValue;

#[derive(Clone, Debug, PartialEq)]
pub struct DynSolParam {
    pub name: String,
    pub value: DynSolValue,
}
