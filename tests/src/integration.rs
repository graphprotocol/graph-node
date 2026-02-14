//! Shared integration test infrastructure.
//!
//! This module contains the test harness types and helper functions used by
//! all integration test binaries (`integration_tests`, `gnd_cli_tests`,
//! `gnd_tests`).

use std::future::Future;
use std::pin::Pin;

use anyhow::{anyhow, bail, Context, Result};
use graph::prelude::serde_json::Value;
use tokio::process::Child;
use tokio::task::JoinError;

use crate::contract::Contract;
use crate::subgraph::Subgraph;
use crate::{error, status};

pub type TestFn = Box<
    dyn FnOnce(TestContext) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>>
        + Sync
        + Send,
>;

pub struct TestContext {
    pub subgraph: Subgraph,
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
        matches!(self.status, TestStatus::Ok)
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
    fn new(s: &str) -> Self {
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

    pub fn new_with_grafting<T>(name: &str, test: fn(TestContext) -> T, base_subgraph: &str) -> Self
    where
        T: Future<Output = Result<(), anyhow::Error>> + Send + 'static,
    {
        let mut test_case = Self::new(name, test);
        test_case.source_subgraph = Some(vec![SourceSubgraph::new(base_subgraph)]);
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
                .map(SourceSubgraph::new)
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
        let subgraph_name = match Subgraph::deploy(subgraph_name, contracts, None).await {
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
        // If a subgraph has subgraph datasources, prepare them first and collect their deployment hashes
        let source_mappings = if let Some(_subgraphs) = &self.source_subgraph {
            match self.prepare_multiple_sources(contracts).await {
                Ok(mappings) => Some(mappings),
                Err(e) => {
                    error!(&self.name, "source subgraph deployment failed: {:?}", e);
                    return Err(e);
                }
            }
        } else {
            None
        };

        status!(&self.name, "Preparing subgraph");
        let (_, subgraph_name, _) =
            match Subgraph::prepare(&self.name, contracts, source_mappings.as_deref()).await {
                Ok(name) => name,
                Err(e) => {
                    error!(&self.name, "Prepare failed: {:?}", e);
                    return Err(e);
                }
            };

        Ok(subgraph_name)
    }

    pub async fn check_health_and_test(self, subgraph_name: String) -> TestResult {
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

    pub async fn run(self, contracts: &[Contract]) -> TestResult {
        // If a subgraph has subgraph datasources, deploy them first and collect their deployment hashes
        let source_mappings = if let Some(_subgraphs) = &self.source_subgraph {
            match self.deploy_multiple_sources(contracts).await {
                Ok(mappings) => Some(mappings),
                Err(e) => {
                    error!(&self.name, "source subgraph deployment failed");
                    return TestResult {
                        name: self.name.clone(),
                        subgraph: None,
                        status: TestStatus::Err(e),
                    };
                }
            }
        } else {
            None
        };

        status!(&self.name, "Deploying subgraph");
        let subgraph_name =
            match Subgraph::deploy(&self.name, contracts, source_mappings.as_deref()).await {
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

        self.check_health_and_test(subgraph_name).await
    }

    async fn prepare_multiple_sources(
        &self,
        contracts: &[Contract],
    ) -> Result<Vec<(String, String)>> {
        let mut mappings = Vec::new();
        if let Some(sources) = &self.source_subgraph {
            for source in sources {
                // Source subgraphs don't have their own sources, so pass None
                let _ = Subgraph::prepare(source.test_name(), contracts, None).await?;
                // If the source has an alias (pre-known IPFS hash), use it for the mapping
                if let Some(alias) = source.alias() {
                    mappings.push((source.test_name().to_string(), alias.to_string()));
                }
            }
        }
        Ok(mappings)
    }

    async fn deploy_multiple_sources(
        &self,
        contracts: &[Contract],
    ) -> Result<Vec<(String, String)>> {
        let mut mappings = Vec::new();
        if let Some(sources) = &self.source_subgraph {
            for source in sources {
                let subgraph = self.deploy_and_wait(source.test_name(), contracts).await?;
                status!(
                    source.test_name(),
                    "Source subgraph deployed with hash {}",
                    subgraph.deployment
                );
                // Use the test_name as the placeholder key
                mappings.push((source.test_name().to_string(), subgraph.deployment.clone()));
            }
        }
        Ok(mappings)
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

pub async fn stop_graph_node(child: &mut Child) -> anyhow::Result<()> {
    child.kill().await.context("Failed to kill graph-node")?;

    Ok(())
}
