use super::test::WasmInstanceExt;
use crate::protobuf::{self, *};
use graph::prelude::tokio;

#[tokio::test]
async fn test_bool_ok() {
    let api_version = super::test::API_VERSION_0_0_5;

    let mut module = super::test::test_module(
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

    let new_obj = module.asc_new(&parm).unwrap();

    let func = module.get_func("my_test").typed().unwrap().clone();

    let res: Result<(), _> = func.call(new_obj.wasm_ptr());

    assert!(res.is_ok(), "{:?}", res.err());
}
