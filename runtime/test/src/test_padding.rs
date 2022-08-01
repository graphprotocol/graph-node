use super::test::WasmInstanceExt;
use crate::protobuf::{self, *};
use graph::prelude::tokio;

//========= this declaration is in protobuf module ============
// #[graph_runtime_derive::generate_asc_type()]
// #[graph_runtime_derive::generate_network_type_id(UnitTestNetwork)]
// #[graph_runtime_derive::generate_from_rust_type()]
// #[graph_runtime_derive::generate_array_type(UnitTestNetwork)]
// #[derive(Debug, PartialEq)]
// pub struct UnitTestType {
//     pub str_pref: String,
//     pub under_test: bool,
//     pub str_suff: String,
// }

#[tokio::test]
async fn test_bool_ok() {
    let api_version = super::test::API_VERSION_0_0_5;

    let module = super::test::test_module(
        "bogus_sub_graph_id",
        super::common::mock_data_source(
            &super::test::wasm_file_path("my_test.wasm", api_version.clone()),
            api_version.clone(),
        ),
        api_version,
    )
    .await;

    let parm = protobuf::UnitTestType {
        str_pref: "pref".into(),
        under_test: true,
        str_suff: "suff".into(),
    };

    let new_obj: graph::runtime::AscPtr<AscUnitTestType> = module.invoke_export1("test_uint", &parm);

    let res: Result<(), _> = module.get_func("my_test").typed().unwrap().call(new_obj);
    assert!(res.is_ok());
}
