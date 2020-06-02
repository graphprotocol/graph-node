use crate::asc_abi::AscPtr;
use wasmtime::Trap;

/// Helper trait for the `link!` macro.
pub(crate) trait IntoWasmRet {
    type Ret: wasmtime::WasmRet;

    fn into_wasm_ret(self) -> Self::Ret;
}

impl IntoWasmRet for () {
    type Ret = Self;
    fn into_wasm_ret(self) -> Self {
        self
    }
}

impl IntoWasmRet for i32 {
    type Ret = Self;
    fn into_wasm_ret(self) -> Self {
        self
    }
}

impl IntoWasmRet for i64 {
    type Ret = Self;
    fn into_wasm_ret(self) -> Self {
        self
    }
}

impl IntoWasmRet for f64 {
    type Ret = Self;
    fn into_wasm_ret(self) -> Self {
        self
    }
}

impl IntoWasmRet for u64 {
    type Ret = i64;
    fn into_wasm_ret(self) -> i64 {
        i64::from_le_bytes(self.to_le_bytes())
    }
}

impl IntoWasmRet for bool {
    type Ret = i32;
    fn into_wasm_ret(self) -> i32 {
        self as i32
    }
}

impl<C> IntoWasmRet for AscPtr<C> {
    type Ret = i32;
    fn into_wasm_ret(self) -> i32 {
        self.wasm_ptr()
    }
}

impl<T> IntoWasmRet for Result<T, Trap>
where
    T: IntoWasmRet,
    T::Ret: wasmtime::WasmTy,
{
    type Ret = Result<T::Ret, Trap>;
    fn into_wasm_ret(self) -> Self::Ret {
        self.map(|x| x.into_wasm_ret())
    }
}
