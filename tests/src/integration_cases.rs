//! Shared integration test case functions.
//!
//! These test functions are used by multiple integration test binaries
//! (`integration_tests`, `gnd_cli_tests`, `gnd_tests`).

use graph::prelude::serde_json::{json, Value};

use crate::integration::{query_succeeds, TestContext};
use crate::subgraph::Subgraph;

pub async fn test_int8(ctx: TestContext) -> anyhow::Result<()> {
    let subgraph = ctx.subgraph;
    assert!(subgraph.healthy);

    let resp = subgraph
        .query(
            "{
        foos_0: foos(orderBy: id, block: { number: 0 }) { id }
        foos(orderBy: id) { id value }
      }",
        )
        .await?;

    let exp = json!({
        "foos_0": [],
        "foos": [
          {
            "id": "0",
            "value": "9223372036854775807",
          },
        ],
    });
    assert_eq!(None, resp.get("errors"));
    assert_eq!(exp, resp["data"]);

    Ok(())
}

pub async fn test_timestamp(ctx: TestContext) -> anyhow::Result<()> {
    let subgraph = ctx.subgraph;
    assert!(subgraph.healthy);

    let resp = subgraph
        .query(
            "{
        foos_0: foos(orderBy: id, block: { number: 0 }) { id }
        foos(orderBy: id) { id value }
      }",
        )
        .await?;

    let exp = json!({
        "foos_0": [],
        "foos": [
          {
            "id": "0",
            "value": "1710837304040956",
          },
        ],
    });
    assert_eq!(None, resp.get("errors"));
    assert_eq!(exp, resp["data"]);

    Ok(())
}

pub async fn test_block_handlers(ctx: TestContext) -> anyhow::Result<()> {
    let subgraph = ctx.subgraph;
    assert!(subgraph.healthy);

    // test non-filtered blockHandler
    let exp = json!({
        "blocks": [
        { "id": "1", "number": "1" },
        { "id": "2", "number": "2" },
        { "id": "3", "number": "3" },
        { "id": "4", "number": "4" },
        { "id": "5", "number": "5" },
        { "id": "6", "number": "6" },
        { "id": "7", "number": "7" },
        { "id": "8", "number": "8" },
        { "id": "9", "number": "9" },
        { "id": "10", "number": "10" },
      ]
    });
    query_succeeds(
        "test non-filtered blockHandler",
        &subgraph,
        "{ blocks(orderBy: number, first: 10) { id number } }",
        exp,
    )
    .await?;

    // test query
    let mut values = Vec::new();
    for i in 0..=10 {
        values.push(json!({ "id": i.to_string(), "value": i.to_string() }));
    }
    let exp = json!({ "foos": Value::Array(values) });
    query_succeeds(
        "test query",
        &subgraph,
        "{ foos(orderBy: value, skip: 1) { id value } }",
        exp,
    )
    .await?;

    // should call intialization handler first
    let exp = json!({
      "foo": { "id": "initialize", "value": "-1" },
    });
    query_succeeds(
        "should call intialization handler first",
        &subgraph,
        "{ foo( id: \"initialize\" ) { id value } }",
        exp,
    )
    .await?;

    // test blockHandler with polling filter
    let exp = json!({
        "blockFromPollingHandlers": [
        { "id": "1", "number": "1" },
        { "id": "4", "number": "4" },
        { "id": "7", "number": "7" },
      ]
    });
    query_succeeds(
        "test blockHandler with polling filter",
        &subgraph,
        "{ blockFromPollingHandlers(orderBy: number, first: 3) { id number } }",
        exp,
    )
    .await?;

    // test other blockHandler with polling filter
    let exp = json!({
        "blockFromOtherPollingHandlers": [
        { "id": "2", "number": "2" },
        { "id": "4", "number": "4" },
        { "id": "6", "number": "6" },
      ]
    });
    query_succeeds(
        "test other blockHandler with polling filter",
        &subgraph,
        "{ blockFromOtherPollingHandlers(orderBy: number, first: 3) { id number } }",
        exp,
    )
    .await?;

    // test initialization handler
    let exp = json!({
        "initializes": [
        { "id": "1", "block": "1" },
      ]
    });
    query_succeeds(
        "test initialization handler",
        &subgraph,
        "{ initializes(orderBy: block, first: 10) { id block } }",
        exp,
    )
    .await?;

    // test subgraphFeatures endpoint returns handlers correctly
    let subgraph_features = Subgraph::query_with_vars(
        "query GetSubgraphFeatures($deployment: String!) {
          subgraphFeatures(subgraphId: $deployment) {
            specVersion
            apiVersion
            features
            dataSources
            network
            handlers
          }
        }",
        json!({ "deployment": subgraph.deployment }),
    )
    .await?;
    let handlers = &subgraph_features["data"]["subgraphFeatures"]["handlers"];
    assert!(
        handlers.is_array(),
        "subgraphFeatures.handlers must be an array"
    );
    let handlers = handlers.as_array().unwrap();
    for handler in [
        "block_filter_polling",
        "block_filter_once",
        "block",
        "event",
    ] {
        assert!(
            handlers.contains(&Value::String(handler.to_string())),
            "handlers {:?} must contain {}",
            handlers,
            handler
        );
    }

    Ok(())
}

pub async fn subgraph_data_sources(ctx: TestContext) -> anyhow::Result<()> {
    let subgraph = ctx.subgraph;
    assert!(subgraph.healthy);
    let expected_response = json!({
        "mirrorBlocks": [
            { "id": "1-v1", "number": "1", "testMessage": null },
            { "id": "1-v2", "number": "1", "testMessage": null },
            { "id": "1-v3", "number": "1", "testMessage": "1-message" },
            { "id": "2-v1", "number": "2", "testMessage": null },
            { "id": "2-v2", "number": "2", "testMessage": null },
            { "id": "2-v3", "number": "2", "testMessage": "2-message" },
            { "id": "3-v1", "number": "3", "testMessage": null },
            { "id": "3-v2", "number": "3", "testMessage": null },
            { "id": "3-v3", "number": "3", "testMessage": "3-message" },
        ]
    });

    query_succeeds(
        "Query all blocks with testMessage",
        &subgraph,
        "{ mirrorBlocks(where: {number_lte: 3}, orderBy: number) { id, number, testMessage } }",
        expected_response,
    )
    .await?;

    let expected_response = json!({
        "mirrorBlock": { "id": "1-v3", "number": "1", "testMessage": "1-message" },
    });

    query_succeeds(
        "Query specific block with testMessage",
        &subgraph,
        "{ mirrorBlock(id: \"1-v3\") { id, number, testMessage } }",
        expected_response,
    )
    .await?;

    Ok(())
}

pub async fn test_value_roundtrip(ctx: TestContext) -> anyhow::Result<()> {
    let subgraph = ctx.subgraph;
    assert!(subgraph.healthy);

    let exp = json!({
        "foos": [{ "id": "0", "value": "bla" }],
        "foos_0": []
    });

    let query = "{
        foos_0: foos(orderBy: id, block: { number: 0 }) { id }
        foos(orderBy: id) { id value }
      }";

    query_succeeds("test query", &subgraph, query, exp).await?;

    Ok(())
}

pub async fn test_multiple_subgraph_datasources(ctx: TestContext) -> anyhow::Result<()> {
    let subgraph = ctx.subgraph;
    assert!(subgraph.healthy);

    // Test querying data aggregated from multiple sources
    let exp = json!({
        "aggregatedDatas": [
            {
                "id": "0",
                "sourceA": "from source A",
                "sourceB": "from source B",
                "first": "sourceA"
            },
        ]
    });

    query_succeeds(
        "should aggregate data from multiple sources",
        &subgraph,
        "{ aggregatedDatas(first: 1) { id sourceA sourceB first } }",
        exp,
    )
    .await?;

    Ok(())
}
