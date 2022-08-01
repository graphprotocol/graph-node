#![cfg(test)]
pub mod common;
mod test;

#[cfg(test)]
pub mod test_padding;

pub mod protobuf {
    use wasmtime::WasmParams;



    #[graph_runtime_derive::generate_asc_type()]
    #[graph_runtime_derive::generate_network_type_id(UnitTestNetwork)]
    #[graph_runtime_derive::generate_from_rust_type()]
    #[graph_runtime_derive::generate_array_type(UnitTestNetwork)]
    #[derive(Debug, PartialEq)]
    pub struct UnitTestType {
        pub str_pref: String,
        pub under_test: bool,
        pub str_suff: String,
    }

    //unsafe impl WasmParams for UnitTestType{};


    // impl FromAscObj<AscUnitTestType> for UnitTestType {
    //     fn from_asc_obj<H>(
    //         from: AscUnitTestType,
    //         heap: &H,
    //         gas: &GasCounter,
    //     ) -> Result<Self, DeterministicHostError>
    //     where
    //         H: AscHeap + ?Sized,
    //     {
    //         todo!()
    //     }
    // }
}
