use graph::object;
use graph_tests::{
    fixture::{
        self,
        ethereum::{chain, empty_block, genesis},
        test_ptr,
    },
    recipe::RunnerTestRecipe,
};

#[tokio::test]
async fn file_link_resolver() -> anyhow::Result<()> {
    std::env::set_var("GRAPH_NODE_DISABLE_DEPLOYMENT_HASH_VALIDATION", "true");
    let RunnerTestRecipe { stores, test_info } = RunnerTestRecipe::new_with_file_link_resolver(
        "file_link_resolver",
        "file-link-resolver",
        "subgraph.yaml",
    )
    .await;

    let blocks = {
        let block_0 = genesis();
        let block_1 = empty_block(block_0.ptr(), test_ptr(1));
        let block_2 = empty_block(block_1.ptr(), test_ptr(2));
        let block_3 = empty_block(block_2.ptr(), test_ptr(3));

        vec![block_0, block_1, block_2, block_3]
    };

    let chain = chain(&test_info.test_name, blocks, &stores, None).await;

    let ctx = fixture::setup_with_file_link_resolver(&test_info, &stores, &chain, None, None).await;
    ctx.start_and_sync_to(test_ptr(3)).await;
    let query = r#"{ blocks(first: 4, orderBy: number) { id, hash } }"#;
    let query_res = ctx.query(query).await.unwrap();

    assert_eq!(
        query_res,
        Some(object! {
            blocks: vec![
                object! {
                    id: test_ptr(0).number.to_string(),
                    hash: format!("0x{}", test_ptr(0).hash_hex()),
                },
                object! {
                    id: test_ptr(1).number.to_string(),
                    hash: format!("0x{}", test_ptr(1).hash_hex()),
                },
                object! {
                    id: test_ptr(2).number.to_string(),
                    hash: format!("0x{}", test_ptr(2).hash_hex()),
                },
                object! {
                    id: test_ptr(3).number.to_string(),
                    hash: format!("0x{}", test_ptr(3).hash_hex()),
                },
            ]
        })
    );

    Ok(())
}
