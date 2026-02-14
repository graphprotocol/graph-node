//! Containeraized integration tests.
//!
//! # On the use of [`tokio::join!`]
//!
//! While linear `.await`s look best, sometimes we don't particularly care
//! about the order of execution and we can thus reduce test execution times by
//! `.await`ing in parallel. [`tokio::join!`] and similar macros can help us
//! with that, at the cost of some readability. As a general rule only a few
//! tasks are really worth parallelizing, and applying this trick
//! indiscriminately will only result in messy code and diminishing returns.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context};
use graph::components::subgraph::{
    ProofOfIndexing, ProofOfIndexingEvent, ProofOfIndexingFinisher, ProofOfIndexingVersion,
};
use graph::data::store::Id;
use graph::entity;
use graph::futures03::StreamExt;
use graph::itertools::Itertools;
use graph::prelude::serde_json::{json, Value};
use graph::prelude::{alloy::primitives::Address, hex, BlockPtr, DeploymentHash};
use graph::schema::InputSchema;
use graph::slog::{o, Discard, Logger};
use graph_tests::contract::Contract;
use graph_tests::integration::{
    query_succeeds, stop_graph_node, TestCase, TestContext, TestResult,
};
use graph_tests::integration_cases::{
    subgraph_data_sources, test_block_handlers, test_int8, test_multiple_subgraph_datasources,
    test_timestamp, test_value_roundtrip,
};
use graph_tests::subgraph::Subgraph;
use graph_tests::{error, status, CONFIG};
use tokio::time::sleep;

const SUBGRAPH_LAST_GRAFTING_BLOCK: i32 = 3;

async fn test_eth_api(ctx: TestContext) -> anyhow::Result<()> {
    let subgraph = ctx.subgraph;
    assert!(subgraph.healthy);

    let expected_response = json!({
        "foo": {
            "id": "1",
            "balance": "10000000000000000000000",
            "hasCode1": false,
            "hasCode2": true,
        }
    });

    query_succeeds(
        "Balance should be right",
        &subgraph,
        "{ foo(id: \"1\") { id balance hasCode1 hasCode2 } }",
        expected_response,
    )
    .await?;

    Ok(())
}

async fn test_topic_filters(ctx: TestContext) -> anyhow::Result<()> {
    let subgraph = ctx.subgraph;
    assert!(subgraph.healthy);

    // Events we need are already emitted from Contract::deploy_all

    let exp = json!({
        "anotherTriggerEntities": [
            {
                "a": "1",
                "b": "2",
                "c": "3",
                "data": "abc",
            },
            {
                "a": "1",
                "b": "1",
                "c": "1",
                "data": "abc",
            },
        ],
    });
    query_succeeds(
        "all overloads of the contract function are called",
        &subgraph,
        "{ anotherTriggerEntities(orderBy: id) {  a b c data } }",
        exp,
    )
    .await?;

    Ok(())
}

async fn test_reverted_calls_are_indexed(ctx: TestContext) -> anyhow::Result<()> {
    let subgraph = ctx.subgraph;
    assert!(subgraph.healthy);

    let exp = json!({
        "calls": [
            {
                "id": "100",
                "reverted": true,
                "returnValue": Value::Null,
            },
            {
                "id": "9",
                "reverted": false,
                "returnValue": "10",
            },
        ],
    });
    query_succeeds(
        "all overloads of the contract function are called",
        &subgraph,
        "{ calls(orderBy: id) { id reverted returnValue } }",
        exp,
    )
    .await?;

    Ok(())
}

async fn test_host_exports(ctx: TestContext) -> anyhow::Result<()> {
    let subgraph = ctx.subgraph;
    assert!(subgraph.healthy);
    Ok(())
}

async fn test_non_fatal_errors(ctx: TestContext) -> anyhow::Result<()> {
    let subgraph = ctx.subgraph;
    assert!(!subgraph.healthy);

    let query = "query GetSubgraphFeatures($deployment: String!) {
        subgraphFeatures(subgraphId: $deployment) {
          specVersion
          apiVersion
          features
          dataSources
          network
          handlers
        }
      }";

    let resp =
        Subgraph::query_with_vars(query, json!({ "deployment" : subgraph.deployment })).await?;
    let subgraph_features = &resp["data"]["subgraphFeatures"];
    let exp = json!({
      "specVersion": "0.0.4",
      "apiVersion": "0.0.6",
      "features": ["nonFatalErrors"],
      "dataSources": ["ethereum/contract"],
      "handlers": ["block"],
      "network": "test",
    });
    assert_eq!(&exp, subgraph_features);

    let resp = subgraph
        .query("{ foos(orderBy: id, subgraphError: allow) { id } }")
        .await?;
    let exp = json!([ { "message": "indexing_error" }]);
    assert_eq!(&exp, &resp["errors"]);

    // Importantly, "1" and "11" are not present because their handlers erroed.
    let exp = json!({
                "foos": [
                    { "id": "0" },
                    { "id": "00" }]});
    assert_eq!(&exp, &resp["data"]);

    Ok(())
}

async fn test_overloaded_functions(ctx: TestContext) -> anyhow::Result<()> {
    let subgraph = ctx.subgraph;
    // all overloads of the contract function are called
    assert!(subgraph.healthy);

    let exp = json!({
        "calls": [
            {
                "id": "bytes32 -> uint256",
                "value": "256",
            },
            {
                "id": "string -> string",
                "value": "string -> string",
            },
            {
                "id": "uint256 -> string",
                "value": "uint256 -> string",
            },
        ],
    });
    query_succeeds(
        "all overloads of the contract function are called",
        &subgraph,
        "{ calls(orderBy: id) { id value } }",
        exp,
    )
    .await?;
    Ok(())
}

async fn test_remove_then_update(ctx: TestContext) -> anyhow::Result<()> {
    let subgraph = ctx.subgraph;
    assert!(subgraph.healthy);

    let exp = json!({
        "foos": [{ "id": "0", "removed": true, "value": null}]
    });
    let query = "{ foos(orderBy: id) { id value removed } }";
    query_succeeds(
        "all overloads of the contract function are called",
        &subgraph,
        query,
        exp,
    )
    .await?;

    Ok(())
}

async fn test_subgraph_grafting(ctx: TestContext) -> anyhow::Result<()> {
    async fn get_block_hash(block_number: i32) -> Option<String> {
        const FETCH_BLOCK_HASH: &str = r#"
        query blockHashFromNumber($network: String!, $blockNumber: Int!) {
            hash: blockHashFromNumber(
              network: $network,
              blockNumber: $blockNumber,
            ) } "#;
        let vars = json!({
            "network": "test",
            "blockNumber": block_number
        });

        let resp = Subgraph::query_with_vars(FETCH_BLOCK_HASH, vars)
            .await
            .unwrap();
        assert_eq!(None, resp.get("errors"));
        resp["data"]["hash"].as_str().map(|s| s.to_owned())
    }

    let subgraph = ctx.subgraph;

    assert!(subgraph.healthy);

    // Fetch block hashes from the chain
    let mut block_hashes: Vec<String> = Vec::new();
    for i in 1..4 {
        let hash = get_block_hash(i)
            .await
            .ok_or_else(|| anyhow!("Failed to get block hash for block {}", i))?;
        block_hashes.push(hash);
    }

    // The deployment hash is dynamic (depends on the base subgraph's hash)
    let deployment_hash = DeploymentHash::new(&subgraph.deployment).unwrap();

    // Compute the expected POIs using the actual block hashes
    let expected_pois = compute_expected_pois(&deployment_hash, &block_hashes);

    for i in 1..4 {
        let block_hash = &block_hashes[(i - 1) as usize];

        const FETCH_POI: &str = r#"
        query proofOfIndexing($subgraph: String!, $blockNumber: Int!, $blockHash: String!, $indexer: String!) {
            proofOfIndexing(
              subgraph: $subgraph,
              blockNumber: $blockNumber,
              blockHash: $blockHash,
              indexer: $indexer
            ) } "#;

        let zero_addr = "0000000000000000000000000000000000000000";
        let vars = json!({
            "subgraph": subgraph.deployment,
            "blockNumber": i,
            "blockHash": block_hash,
            "indexer": zero_addr,
        });
        let resp = Subgraph::query_with_vars(FETCH_POI, vars).await?;
        assert_eq!(None, resp.get("errors"));
        assert!(resp["data"]["proofOfIndexing"].is_string());
        let poi = resp["data"]["proofOfIndexing"].as_str().unwrap();
        // Check the expected value of the POI. The transition from the old legacy
        // hashing to the new one is done in the block #2 anything before that
        // should not change as the legacy code will not be updated. Any change
        // after that might indicate a change in the way new POI is now calculated.
        // Change on the block #2 would mean a change in the transitioning
        // from the old to the new algorithm hence would be reflected only
        // subgraphs that are grafting from pre 0.0.5 to 0.0.6 or newer.
        assert_eq!(
            poi,
            expected_pois[(i - 1) as usize],
            "POI mismatch for block {}",
            i
        );
    }

    Ok(())
}

/// Compute the expected POI values for the grafted subgraph.
///
/// The grafted subgraph:
/// - Spec version 0.0.6 (uses Fast POI algorithm)
/// - Grafts from base subgraph at block 2
/// - Creates GraftedData entities starting from block 3
///
/// The base subgraph:
/// - Spec version 0.0.5 (uses Legacy POI algorithm)
/// - Creates BaseData entities for each block
///
/// POI algorithm transition:
/// - Blocks 0-2: Legacy POI digests (from base subgraph)
/// - Block 3+: Fast POI algorithm with transition from Legacy
fn compute_expected_pois(deployment_hash: &DeploymentHash, block_hashes: &[String]) -> Vec<String> {
    let logger = Logger::root(Discard, o!());
    let causality_region = "ethereum/test";

    // Create schemas for the entity types
    let base_schema = InputSchema::parse_latest(
        "type BaseData @entity(immutable: true) { id: ID!, data: String!, blockNumber: BigInt! }",
        deployment_hash.clone(),
    )
    .unwrap();

    let grafted_schema = InputSchema::parse_latest(
        "type GraftedData @entity(immutable: true) { id: ID!, data: String!, blockNumber: BigInt! }",
        deployment_hash.clone(),
    )
    .unwrap();

    // Compute POI digests at each block checkpoint
    // Store the accumulated state after each block so we can compute POI at any block
    let mut db_at_block: HashMap<i32, HashMap<Id, Vec<u8>>> = HashMap::new();
    let mut db: HashMap<Id, Vec<u8>> = HashMap::new();

    // Process blocks 0-3:
    // - Blocks 0-2: Legacy POI (from base subgraph, creates BaseData entities)
    // - Block 3: Fast POI (grafted subgraph starts here, creates GraftedData entity)
    //
    // The base subgraph starts from block 0 (genesis block triggers handlers in Anvil).
    //
    // The grafted subgraph:
    // - spec version 0.0.6 → uses Fast POI algorithm
    // - grafts from base subgraph at block 2
    // - inherits POI digests from base for blocks 0-2
    // - transitions from Legacy to Fast at block 3
    for block_i in 0..=3i32 {
        let version = if block_i <= 2 {
            ProofOfIndexingVersion::Legacy
        } else {
            ProofOfIndexingVersion::Fast
        };

        let mut stream = ProofOfIndexing::new(block_i, version);

        if block_i <= 2 {
            // Base subgraph creates BaseData
            let id_str = block_i.to_string();
            let entity = entity! {
                base_schema =>
                    id: &id_str,
                    data: "from base",
                    blockNumber: graph::prelude::Value::BigInt(block_i.into()),
            };

            let event = ProofOfIndexingEvent::SetEntity {
                entity_type: "BaseData",
                id: &id_str,
                data: &entity,
            };
            stream.write(&logger, causality_region, &event);
        } else {
            // Grafted subgraph creates GraftedData
            let id_str = block_i.to_string();
            let entity = entity! {
                grafted_schema =>
                    id: &id_str,
                    data: "to grafted",
                    blockNumber: graph::prelude::Value::BigInt(block_i.into()),
            };

            let event = ProofOfIndexingEvent::SetEntity {
                entity_type: "GraftedData",
                id: &id_str,
                data: &entity,
            };
            stream.write(&logger, causality_region, &event);
        }

        for (name, region) in stream.take() {
            let prev = db.get(&name);
            let update = region.pause(prev.map(|v| &v[..]));
            db.insert(name, update);
        }

        db_at_block.insert(block_i, db.clone());
    }

    // Compute POI for blocks 1, 2, 3
    let mut pois = Vec::new();
    for (block_idx, block_hash_hex) in block_hashes.iter().enumerate() {
        let block_number = (block_idx + 1) as i32;

        // Get the POI version for this block - grafted subgraph uses Fast (spec 0.0.6)
        let version = ProofOfIndexingVersion::Fast;

        let block_hash_bytes = hex::decode(block_hash_hex).unwrap();
        let block_ptr = BlockPtr::from((block_hash_bytes, block_number as u64));

        // Use zero address to match the test query's indexer parameter
        let indexer = Some(Address::ZERO);
        let mut finisher =
            ProofOfIndexingFinisher::new(&block_ptr, deployment_hash, &indexer, version);

        if let Some(db_state) = db_at_block.get(&block_number) {
            for (name, region) in db_state.iter() {
                finisher.add_causality_region(name, region);
            }
        }

        let poi_bytes = finisher.finish();
        let poi = format!("0x{}", hex::encode(poi_bytes));
        pois.push(poi);
    }

    pois
}

async fn test_poi_for_failed_subgraph(ctx: TestContext) -> anyhow::Result<()> {
    let subgraph = ctx.subgraph;
    const INDEXING_STATUS: &str = r#"
    query statuses($subgraphName: String!) {
        statuses: indexingStatusesForSubgraphName(subgraphName: $subgraphName) {
          subgraph
          health
          entityCount
          chains {
            network
            latestBlock { number hash }
          } } }"#;

    const FETCH_POI: &str = r#"
    query proofOfIndexing($subgraph: String!, $blockNumber: Int!, $blockHash: String!) {
        proofOfIndexing(
          subgraph: $subgraph,
          blockNumber: $blockNumber,
          blockHash: $blockHash
        ) } "#;

    // Wait up to 5 minutes for the subgraph to write the failure
    const STATUS_WAIT: Duration = Duration::from_secs(300);

    assert!(!subgraph.healthy);

    struct Status {
        health: String,
        entity_count: String,
        latest_block: Value,
    }

    async fn fetch_status(subgraph: &Subgraph) -> anyhow::Result<Status> {
        let resp =
            Subgraph::query_with_vars(INDEXING_STATUS, json!({ "subgraphName": subgraph.name }))
                .await?;
        assert_eq!(None, resp.get("errors"));
        let statuses = &resp["data"]["statuses"];
        assert_eq!(1, statuses.as_array().unwrap().len());
        let status = &statuses[0];
        let health = status["health"].as_str().unwrap();
        let entity_count = status["entityCount"].as_str().unwrap();
        let latest_block = &status["chains"][0]["latestBlock"];
        Ok(Status {
            health: health.to_string(),
            entity_count: entity_count.to_string(),
            latest_block: latest_block.clone(),
        })
    }

    let start = Instant::now();
    let status = {
        let mut status = fetch_status(&subgraph).await?;
        while status.latest_block.is_null() && start.elapsed() < STATUS_WAIT {
            sleep(Duration::from_secs(1)).await;
            status = fetch_status(&subgraph).await?;
        }
        status
    };
    if status.latest_block.is_null() {
        bail!("Subgraph never wrote the failed block");
    }

    assert_eq!("1", status.entity_count);
    assert_eq!("failed", status.health);

    let calls = subgraph
        .query("{ calls(subgraphError: allow)  { id value } }")
        .await?;
    // We have indexing errors
    assert!(calls.get("errors").is_some());

    let calls = &calls["data"]["calls"];
    assert_eq!(0, calls.as_array().unwrap().len());

    let block_number: u64 = status.latest_block["number"].as_str().unwrap().parse()?;
    let vars = json!({
        "subgraph": subgraph.deployment,
        "blockNumber": block_number,
        "blockHash": status.latest_block["hash"],
    });
    let resp = Subgraph::query_with_vars(FETCH_POI, vars).await?;
    assert_eq!(None, resp.get("errors"));
    assert!(resp["data"]["proofOfIndexing"].is_string());
    Ok(())
}

#[allow(dead_code)]
async fn test_missing(_sg: Subgraph) -> anyhow::Result<()> {
    Err(anyhow!("This test is missing"))
}

/// Test the declared calls functionality as of spec version 1.2.0.
/// Note that we don't have a way to test that the actual call is made as
/// a declared call since graph-node does not expose that information
/// to mappings. This test assures though that the declared call machinery
/// does not have any errors.
async fn test_declared_calls_basic(ctx: TestContext) -> anyhow::Result<()> {
    #[track_caller]
    fn assert_call_result(call_results: &[Value], label: &str, exp_success: bool, exp_value: &str) {
        let Some(call_result) = call_results.iter().find(|c| c["label"] == json!(label)) else {
            panic!(
                "Expected call result with label '{}', but none found",
                label
            );
        };
        let Some(act_success) = call_result["success"].as_bool() else {
            panic!(
                "Expected call result with label '{}' to have a boolean 'success' field, but got: {:?}",
                label, call_result["success"]
            );
        };

        if exp_success {
            assert!(
                act_success,
                "Expected call result with label '{}' to be successful",
                label
            );
            let Some(act_value) = call_result["value"].as_str() else {
                panic!(
                "Expected call result with label '{}' to have a string 'value' field, but got: {:?}",
                label, call_result["value"]
            );
            };
            assert_eq!(
                exp_value, act_value,
                "Expected call result with label '{}' to have value '{}', but got '{}'",
                label, exp_value, act_value
            );
        } else {
            assert!(
                !act_success,
                "Expected call result with label '{}' to have failed",
                label
            );
        }
    }

    let subgraph = ctx.subgraph;
    assert!(subgraph.healthy);

    // Query the results
    const QUERY: &str = "{
        transferCalls(first: 1, orderBy: blockNumber) {
            id
            from
            to
            value
            balanceFromBefore
            balanceToBefore
            totalSupply
            constantValue
            sumResult
            metadataFrom
            revertCallSucceeded
        }
        callResults(orderBy: label) {
            label
            success
            value
            error
        }
    }";

    let Some((transfer_calls, call_results)) = subgraph
        .polling_query(QUERY, &["transferCalls", "callResults"])
        .await?
        .into_iter()
        .collect_tuple()
    else {
        panic!("Expected exactly two arrays from polling_query")
    };

    // Validate basic functionality
    assert!(
        !transfer_calls.is_empty(),
        "Should have at least one transfer call"
    );
    assert!(!call_results.is_empty(), "Should have call results");

    let transfer_call = &transfer_calls[0];

    // Validate declared calls worked
    assert_eq!(
        transfer_call["constantValue"],
        json!("42"),
        "Constant value should be 42"
    );
    assert_eq!(
        transfer_call["sumResult"],
        json!("200"),
        "Sum result should be 200 (100 + 100)"
    );
    assert_eq!(
        transfer_call["revertCallSucceeded"],
        json!(false),
        "Revert call should have failed"
    );
    assert_eq!(
        transfer_call["totalSupply"],
        json!("3000"),
        "Total supply should be 3000"
    );

    assert_call_result(&call_results, "balance_from", true, "900");
    assert_call_result(&call_results, "balance_to", true, "1100");
    assert_call_result(&call_results, "constant_value", true, "42");
    assert_call_result(&call_results, "metadata_from", true, "Test Asset 1");
    assert_call_result(&call_results, "sum_values", true, "200");
    assert_call_result(&call_results, "total_supply", true, "3000");
    assert_call_result(&call_results, "will_revert", false, "*ignored*");

    Ok(())
}

async fn test_declared_calls_struct_fields(ctx: TestContext) -> anyhow::Result<()> {
    let subgraph = ctx.subgraph;
    assert!(subgraph.healthy);

    // Wait a moment for indexing
    sleep(Duration::from_secs(2)).await;

    // Query the results
    const QUERY: &str = "{
        assetTransferCalls(first: 1, orderBy: blockNumber) {
            id
            assetAddr
            assetAmount
            assetActive
            owner
            metadata
            amountCalc
        }
        complexAssetCalls(first: 1, orderBy: blockNumber) {
            id
            baseAssetAddr
            baseAssetAmount
            baseAssetOwner
            baseAssetMetadata
            baseAssetAmountCalc
        }
        structFieldTests(orderBy: testType) {
            testType
            fieldName
            success
            result
            error
        }
    }";

    let Some((asset_transfers, complex_assets, struct_tests)) = subgraph
        .polling_query(
            QUERY,
            &[
                "assetTransferCalls",
                "complexAssetCalls",
                "structFieldTests",
            ],
        )
        .await?
        .into_iter()
        .collect_tuple()
    else {
        panic!("Expected exactly three arrays from polling_query")
    };

    // Validate struct field access
    assert!(
        !asset_transfers.is_empty(),
        "Should have asset transfer calls"
    );
    assert!(
        !complex_assets.is_empty(),
        "Should have complex asset calls"
    );
    assert!(!struct_tests.is_empty(), "Should have struct field tests");

    let asset_transfer = &asset_transfers[0];

    // Validate struct field values
    assert_eq!(
        asset_transfer["assetAddr"],
        json!("0x1111111111111111111111111111111111111111")
    );
    assert_eq!(asset_transfer["assetAmount"], json!("150"));
    assert_eq!(asset_transfer["assetActive"], json!(true));
    assert_eq!(asset_transfer["amountCalc"], json!("300")); // 150 + 150

    // Validate complex asset (nested struct access)
    let complex_asset = &complex_assets[0];
    assert_eq!(
        complex_asset["baseAssetAddr"],
        json!("0x4444444444444444444444444444444444444444")
    );
    assert_eq!(complex_asset["baseAssetAmount"], json!("250"));
    assert_eq!(complex_asset["baseAssetAmountCalc"], json!("349")); // 250 + 99

    // Validate that struct field tests include both successful calls
    let successful_tests: Vec<_> = struct_tests
        .iter()
        .filter(|t| t["success"] == json!(true))
        .collect();
    assert!(
        !successful_tests.is_empty(),
        "Should have successful struct field tests"
    );

    Ok(())
}

/// The main test entrypoint.
#[graph::test]
async fn integration_tests() -> anyhow::Result<()> {
    let test_name_to_run = std::env::var("TEST_CASE").ok();

    let cases = vec![
        TestCase::new("reverted-calls", test_reverted_calls_are_indexed),
        TestCase::new("host-exports", test_host_exports),
        TestCase::new("non-fatal-errors", test_non_fatal_errors),
        TestCase::new("overloaded-functions", test_overloaded_functions),
        TestCase::new("poi-for-failed-subgraph", test_poi_for_failed_subgraph),
        TestCase::new("remove-then-update", test_remove_then_update),
        TestCase::new("value-roundtrip", test_value_roundtrip),
        TestCase::new("int8", test_int8),
        TestCase::new("block-handlers", test_block_handlers),
        TestCase::new("timestamp", test_timestamp),
        TestCase::new("ethereum-api-tests", test_eth_api),
        TestCase::new("topic-filter", test_topic_filters),
        TestCase::new_with_grafting("grafted", test_subgraph_grafting, "base"),
        TestCase::new_with_source_subgraphs(
            "subgraph-data-sources",
            subgraph_data_sources,
            vec!["source-subgraph"],
        ),
        TestCase::new_with_source_subgraphs(
            "multiple-subgraph-datasources",
            test_multiple_subgraph_datasources,
            vec!["source-subgraph-a", "source-subgraph-b"],
        ),
        TestCase::new("declared-calls-basic", test_declared_calls_basic),
        TestCase::new(
            "declared-calls-struct-fields",
            test_declared_calls_struct_fields,
        ),
    ];

    // Filter the test cases if a specific test name is provided
    let cases_to_run: Vec<_> = if let Some(test_name) = test_name_to_run {
        cases
            .into_iter()
            .filter(|case| case.name == test_name)
            .collect()
    } else {
        cases
    };

    // Mine empty blocks to reach the required block number for grafting tests.
    // This ensures deterministic block hashes for blocks 1-3 before any
    // contract deployments occur.
    status!(
        "setup",
        "Mining blocks to reach block {}",
        SUBGRAPH_LAST_GRAFTING_BLOCK
    );
    Contract::ensure_block(SUBGRAPH_LAST_GRAFTING_BLOCK as u64)
        .await
        .context("Failed to mine initial blocks for grafting test")?;

    let contracts = Contract::deploy_all().await?;

    status!("setup", "Resetting database");
    CONFIG.reset_database();

    // Spawn graph-node.
    status!("graph-node", "Starting graph-node");
    let mut graph_node_child_command = CONFIG.spawn_graph_node().await?;

    let stream = tokio_stream::iter(cases_to_run)
        .map(|case| case.run(&contracts))
        .buffered(CONFIG.num_parallel_tests);

    let mut results: Vec<TestResult> = stream.collect::<Vec<_>>().await;
    results.sort_by_key(|result| result.name.clone());

    // Stop graph-node and read its output.
    let graph_node_res = stop_graph_node(&mut graph_node_child_command).await;

    status!(
        "graph-node",
        "graph-node logs are in {}",
        CONFIG.graph_node.log_file.path.display()
    );

    match graph_node_res {
        Ok(_) => {
            status!("graph-node", "Stopped graph-node");
        }
        Err(e) => {
            error!("graph-node", "Failed to stop graph-node: {}", e);
        }
    }

    println!("\n\n{:=<60}", "");
    println!("Test results:");
    println!("{:-<60}", "");
    for result in &results {
        result.print();
    }
    println!("\n");

    if results.iter().any(|result| !result.success()) {
        Err(anyhow!("Some tests failed"))
    } else {
        Ok(())
    }
}
