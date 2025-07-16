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

use std::future::Future;
use std::pin::Pin;
use std::time::{self, Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use graph::futures03::StreamExt;
use graph::prelude::serde_json::{json, Value};
use graph::prelude::web3::types::U256;
use graph_tests::contract::Contract;
use graph_tests::helpers::{run_checked, TestFile};
use graph_tests::subgraph::Subgraph;
use graph_tests::{error, status, CONFIG};
use tokio::process::{Child, Command};
use tokio::task::JoinError;
use tokio::time::sleep;

const SUBGRAPH_LAST_GRAFTING_BLOCK: i32 = 3;

type TestFn = Box<
    dyn FnOnce(TestContext) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>>
        + Sync
        + Send,
>;

pub struct TestContext {
    pub subgraph: Subgraph,
    pub contracts: Vec<Contract>,
}

pub enum TestStatus {
    Ok,
    Err(anyhow::Error),
    Panic(JoinError),
}

pub struct TestResult {
    pub name: String,
    pub subgraph: Option<Subgraph>,
    pub status: TestStatus,
}

impl TestResult {
    pub fn success(&self) -> bool {
        match self.status {
            TestStatus::Ok => true,
            _ => false,
        }
    }

    fn print_subgraph(&self) {
        if let Some(subgraph) = &self.subgraph {
            println!("    Subgraph: {}", subgraph.deployment);
        }
    }

    pub fn print(&self) {
        // ANSI escape sequences; see the comment in macros.rs about better colorization
        const GREEN: &str = "\x1b[1;32m";
        const RED: &str = "\x1b[1;31m";
        const NC: &str = "\x1b[0m";

        match &self.status {
            TestStatus::Ok => {
                println!("* {GREEN}Test {} succeeded{NC}", self.name);
                self.print_subgraph();
            }
            TestStatus::Err(e) => {
                println!("* {RED}Test {} failed{NC}", self.name);
                self.print_subgraph();
                println!("    {:?}", e);
            }
            TestStatus::Panic(e) => {
                if e.is_cancelled() {
                    println!("* {RED}Test {} was cancelled{NC}", self.name)
                } else if e.is_panic() {
                    println!("* {RED}Test {} failed{NC}", self.name);
                } else {
                    println!("* {RED}Test {} exploded mysteriously{NC}", self.name)
                }
                self.print_subgraph();
            }
        }
    }
}

#[derive(Debug, Clone)]
pub enum SourceSubgraph {
    Subgraph(String),
    WithAlias((String, String)), // (alias, test_name)
}

impl SourceSubgraph {
    pub fn from_str(s: &str) -> Self {
        if let Some((alias, subgraph)) = s.split_once(':') {
            Self::WithAlias((alias.to_string(), subgraph.to_string()))
        } else {
            Self::Subgraph(s.to_string())
        }
    }

    pub fn test_name(&self) -> &str {
        match self {
            Self::Subgraph(name) => name,
            Self::WithAlias((_, name)) => name,
        }
    }

    pub fn alias(&self) -> Option<&str> {
        match self {
            Self::Subgraph(_) => None,
            Self::WithAlias((alias, _)) => Some(alias),
        }
    }
}

pub struct TestCase {
    pub name: String,
    pub test: TestFn,
    pub source_subgraph: Option<Vec<SourceSubgraph>>,
}

impl TestCase {
    pub fn new<T>(name: &str, test: fn(TestContext) -> T) -> Self
    where
        T: Future<Output = Result<(), anyhow::Error>> + Send + 'static,
    {
        Self {
            name: name.to_string(),
            test: Box::new(move |ctx| Box::pin(test(ctx))),
            source_subgraph: None,
        }
    }

    fn new_with_grafting<T>(name: &str, test: fn(TestContext) -> T, base_subgraph: &str) -> Self
    where
        T: Future<Output = Result<(), anyhow::Error>> + Send + 'static,
    {
        let mut test_case = Self::new(name, test);
        test_case.source_subgraph = Some(vec![SourceSubgraph::from_str(base_subgraph)]);
        test_case
    }

    pub fn new_with_source_subgraphs<T>(
        name: &str,
        test: fn(TestContext) -> T,
        source_subgraphs: Vec<&str>,
    ) -> Self
    where
        T: Future<Output = Result<(), anyhow::Error>> + Send + 'static,
    {
        let mut test_case = Self::new(name, test);
        test_case.source_subgraph = Some(
            source_subgraphs
                .into_iter()
                .map(SourceSubgraph::from_str)
                .collect(),
        );
        test_case
    }

    async fn deploy_and_wait(
        &self,
        subgraph_name: &str,
        contracts: &[Contract],
    ) -> Result<Subgraph> {
        status!(&self.name, "Deploying subgraph");
        let subgraph_name = match Subgraph::deploy(&subgraph_name, contracts).await {
            Ok(name) => name,
            Err(e) => {
                error!(&self.name, "Deploy failed");
                return Err(anyhow!(e.context("Deploy failed")));
            }
        };

        status!(&self.name, "Waiting for subgraph to become ready");
        let subgraph = match Subgraph::wait_ready(&subgraph_name).await {
            Ok(subgraph) => subgraph,
            Err(e) => {
                error!(&self.name, "Subgraph never synced or failed");
                return Err(anyhow!(e.context("Subgraph never synced or failed")));
            }
        };

        if subgraph.healthy {
            status!(&self.name, "Subgraph ({}) is synced", subgraph.deployment);
        } else {
            status!(&self.name, "Subgraph ({}) has failed", subgraph.deployment);
        }

        Ok(subgraph)
    }

    pub async fn prepare(&self, contracts: &[Contract]) -> anyhow::Result<String> {
        // If a subgraph has subgraph datasources, prepare them first
        if let Some(_subgraphs) = &self.source_subgraph {
            if let Err(e) = self.prepare_multiple_sources(contracts).await {
                error!(&self.name, "source subgraph deployment failed: {:?}", e);
                return Err(e);
            }
        }

        status!(&self.name, "Preparing subgraph");
        let (_, subgraph_name, _) = match Subgraph::prepare(&self.name, contracts).await {
            Ok(name) => name,
            Err(e) => {
                error!(&self.name, "Prepare failed: {:?}", e);
                return Err(e);
            }
        };

        Ok(subgraph_name)
    }

    pub async fn check_health_and_test(
        self,
        contracts: &[Contract],
        subgraph_name: String,
    ) -> TestResult {
        status!(
            &self.name,
            "Waiting for subgraph ({}) to become ready",
            subgraph_name
        );
        let subgraph = match Subgraph::wait_ready(&subgraph_name).await {
            Ok(subgraph) => subgraph,
            Err(e) => {
                error!(&self.name, "Subgraph never synced or failed");
                return TestResult {
                    name: self.name.clone(),
                    subgraph: None,
                    status: TestStatus::Err(e.context("Subgraph never synced or failed")),
                };
            }
        };

        if subgraph.healthy {
            status!(&self.name, "Subgraph ({}) is synced", subgraph.deployment);
        } else {
            status!(&self.name, "Subgraph ({}) has failed", subgraph.deployment);
        }

        let ctx = TestContext {
            subgraph: subgraph.clone(),
            contracts: contracts.to_vec(),
        };

        status!(&self.name, "Starting test");
        let subgraph2 = subgraph.clone();
        let res = tokio::spawn(async move { (self.test)(ctx).await }).await;
        let status = match res {
            Ok(Ok(())) => {
                status!(&self.name, "Test succeeded");
                TestStatus::Ok
            }
            Ok(Err(e)) => {
                error!(&self.name, "Test failed");
                TestStatus::Err(e)
            }
            Err(e) => {
                error!(&self.name, "Test panicked");
                TestStatus::Panic(e)
            }
        };
        TestResult {
            name: self.name.clone(),
            subgraph: Some(subgraph2),
            status,
        }
    }

    async fn run(self, contracts: &[Contract]) -> TestResult {
        // If a subgraph has subgraph datasources, deploy them first
        if let Some(_subgraphs) = &self.source_subgraph {
            if let Err(e) = self.deploy_multiple_sources(contracts).await {
                error!(&self.name, "source subgraph deployment failed");
                return TestResult {
                    name: self.name.clone(),
                    subgraph: None,
                    status: TestStatus::Err(e),
                };
            }
        }

        status!(&self.name, "Deploying subgraph");
        let subgraph_name = match Subgraph::deploy(&self.name, contracts).await {
            Ok(name) => name,
            Err(e) => {
                error!(&self.name, "Deploy failed");
                return TestResult {
                    name: self.name.clone(),
                    subgraph: None,
                    status: TestStatus::Err(e.context("Deploy failed")),
                };
            }
        };

        self.check_health_and_test(contracts, subgraph_name).await
    }

    async fn prepare_multiple_sources(&self, contracts: &[Contract]) -> Result<()> {
        if let Some(sources) = &self.source_subgraph {
            for source in sources {
                let _ = Subgraph::prepare(source.test_name(), contracts).await?;
            }
        }
        Ok(())
    }

    async fn deploy_multiple_sources(&self, contracts: &[Contract]) -> Result<()> {
        if let Some(sources) = &self.source_subgraph {
            for source in sources {
                let subgraph = self.deploy_and_wait(source.test_name(), contracts).await?;
                status!(
                    source.test_name(),
                    "Source subgraph deployed with hash {}",
                    subgraph.deployment
                );
            }
        }
        Ok(())
    }
}

/// Run the given `query` against the `subgraph` and check that the result
/// has no errors and that the `data` portion of the response matches the
/// `exp` value.
pub async fn query_succeeds(
    title: &str,
    subgraph: &Subgraph,
    query: &str,
    exp: Value,
) -> anyhow::Result<()> {
    let resp = subgraph.query(query).await?;
    match resp.get("errors") {
        None => { /* nothing to do */ }
        Some(errors) => {
            bail!(
                "query for `{}` returned GraphQL errors: {:?}",
                title,
                errors
            );
        }
    }
    match resp.get("data") {
        None => {
            bail!("query for `{}` returned no data", title);
        }
        Some(data) => {
            if &exp != data {
                bail!(
                    "query for `{title}` returned unexpected data:  \nexpected: {exp:?}\n  returned: {data:?}",
                );
            }
        }
    }
    Ok(())
}

/*
* Actual tests. For a new test, add a new function here and add an entry to
* the `cases` variable in `integration_tests`.
*/

async fn test_int8(ctx: TestContext) -> anyhow::Result<()> {
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

/*
* Actual tests. For a new test, add a new function here and add an entry to
* the `cases` variable in `integration_tests`.
*/

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

async fn test_topic_filters(ctx: TestContext) -> anyhow::Result<()> {
    let subgraph = ctx.subgraph;
    assert!(subgraph.healthy);

    let contract = ctx
        .contracts
        .iter()
        .find(|x| x.name == "SimpleContract")
        .unwrap();

    contract
        .call(
            "emitAnotherTrigger",
            (
                U256::from(1),
                U256::from(2),
                U256::from(3),
                "abc".to_string(),
            ),
        )
        .await
        .unwrap();

    contract
        .call(
            "emitAnotherTrigger",
            (
                U256::from(1),
                U256::from(1),
                U256::from(1),
                "abc".to_string(),
            ),
        )
        .await
        .unwrap();

    contract
        .call(
            "emitAnotherTrigger",
            (
                U256::from(4),
                U256::from(2),
                U256::from(3),
                "abc".to_string(),
            ),
        )
        .await
        .unwrap();

    contract
        .call(
            "emitAnotherTrigger",
            (
                U256::from(4),
                U256::from(4),
                U256::from(3),
                "abc".to_string(),
            ),
        )
        .await
        .unwrap();

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

async fn test_value_roundtrip(ctx: TestContext) -> anyhow::Result<()> {
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

    let block_hashes: Vec<&str> = vec![
        "384c705d4d1933ae8ba89026f016f09854057a267e1143e47bb7511d772a35d4",
        "b90423eead33404dae0684169d35edd494b36802b721fb8de0bb8bc036c10480",
        "2a6c4b65d659e0485371a93bc1ac0f0d7bc0f25a454b5f23a842335fea0638d5",
    ];

    let pois: Vec<&str> = vec![
        "0xde9e5650e22e61def6990d3fc4bd5915a4e8e0dd54af0b6830bf064aab16cc03",
        "0x5d790dca3e37bd9976345d32d437b84ba5ea720a0b6ea26231a866e9f078bd52",
        "0x719c04b78e01804c86f2bd809d20f481e146327af07227960e2242da365754ef",
    ];

    for i in 1..4 {
        let block_hash = get_block_hash(i).await.unwrap();
        // We need to make sure that the preconditions for POI are fulfiled
        // namely that the blockchain produced the proper block hashes for the
        // blocks of which we will check the POI.
        assert_eq!(block_hash, block_hashes[(i - 1) as usize]);

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
        assert_eq!(poi, pois[(i - 1) as usize]);
    }

    Ok(())
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

pub async fn test_multiple_subgraph_datasources(ctx: TestContext) -> anyhow::Result<()> {
    let subgraph = ctx.subgraph;
    assert!(subgraph.healthy);

    println!("subgraph: {:?}", subgraph);

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

async fn wait_for_blockchain_block(block_number: i32) -> bool {
    // Wait up to 5 minutes for the expected block to appear
    const STATUS_WAIT: Duration = Duration::from_secs(300);
    const REQUEST_REPEATING: Duration = time::Duration::from_secs(1);
    let start = Instant::now();
    while start.elapsed() < STATUS_WAIT {
        let latest_block = Contract::latest_block().await;
        if let Some(latest_block) = latest_block {
            if let Some(number) = latest_block.number {
                if number >= block_number.into() {
                    return true;
                }
            }
        }
        tokio::time::sleep(REQUEST_REPEATING).await;
    }
    false
}

/// The main test entrypoint.
#[tokio::test]
async fn integration_tests() -> anyhow::Result<()> {
    // Test "api-version-v0-0-4" was commented out in the original; what's
    // up with that?

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

    // Here we wait for a block in the blockchain in order not to influence
    // block hashes for all the blocks until the end of the grafting tests.
    // Currently the last used block for grafting test is the block 3.
    assert!(wait_for_blockchain_block(SUBGRAPH_LAST_GRAFTING_BLOCK).await);

    let contracts = Contract::deploy_all().await?;

    status!("setup", "Resetting database");
    CONFIG.reset_database();

    status!("setup", "Initializing yarn workspace");
    yarn_workspace().await?;

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

pub async fn stop_graph_node(child: &mut Child) -> anyhow::Result<()> {
    child.kill().await.context("Failed to kill graph-node")?;

    Ok(())
}

pub async fn yarn_workspace() -> anyhow::Result<()> {
    // We shouldn't really have to do this since we use the bundled version
    // of graph-cli, but that gets very unhappy if the workspace isn't
    // initialized
    let wsp = TestFile::new("integration-tests");
    run_checked(Command::new("yarn").arg("install").current_dir(&wsp.path)).await?;
    Ok(())
}
