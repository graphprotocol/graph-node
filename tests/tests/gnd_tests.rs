use anyhow::anyhow;
use graph::futures03::StreamExt;
use graph_tests::config::set_dev_mode;
use graph_tests::contract::Contract;
use graph_tests::subgraph::Subgraph;
use graph_tests::{error, status, CONFIG};

mod integration_tests;

use integration_tests::{
    stop_graph_node, subgraph_data_sources, test_block_handlers,
    test_multiple_subgraph_datasources, yarn_workspace, TestCase, TestResult,
};

/// The main test entrypoint.
#[tokio::test]
async fn gnd_tests() -> anyhow::Result<()> {
    set_dev_mode(true);

    let test_name_to_run = std::env::var("TEST_CASE").ok();

    let cases = vec![
        TestCase::new("block-handlers", test_block_handlers),
        TestCase::new_with_source_subgraphs(
            "subgraph-data-sources",
            subgraph_data_sources,
            vec!["QmWi3H11QFE2PiWx6WcQkZYZdA5UasaBptUJqGn54MFux5:source-subgraph"],
        ),
        TestCase::new_with_source_subgraphs(
            "multiple-subgraph-datasources",
            test_multiple_subgraph_datasources,
            vec![
                "QmYHp1bPEf7EoYBpEtJUpZv1uQHYQfWE4AhvR6frjB1Huj:source-subgraph-a",
                "QmYBEzastJi7bsa722ac78tnZa6xNnV9vvweerY4kVyJtq:source-subgraph-b",
            ],
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

    let contracts = Contract::deploy_all().await?;

    status!("setup", "Resetting database");
    CONFIG.reset_database();

    status!("setup", "Initializing yarn workspace");
    yarn_workspace().await?;

    for i in cases_to_run.iter() {
        i.prepare(&contracts).await?;
    }
    status!("setup", "Prepared all cases");

    let manifests = cases_to_run
        .iter()
        .map(|case| {
            Subgraph::dir(&case.name)
                .path
                .join("subgraph.yaml")
                .to_str()
                .unwrap()
                .to_string()
        })
        .collect::<Vec<_>>()
        .join(",");

    let aliases = cases_to_run
        .iter()
        .filter_map(|case| case.source_subgraph.as_ref())
        .flatten()
        .filter_map(|source_subgraph| {
            source_subgraph.alias().map(|alias| {
                let manifest_path = Subgraph::dir(source_subgraph.test_name())
                    .path
                    .join("subgraph.yaml")
                    .to_str()
                    .unwrap()
                    .to_string();
                format!("{}:{}", alias, manifest_path)
            })
        })
        .collect::<Vec<_>>();

    let aliases_str = aliases.join(",");
    let args = if aliases.is_empty() {
        vec!["--manifests", &manifests]
    } else {
        vec!["--manifests", &manifests, "--sources", &aliases_str]
    };

    // Spawn graph-node.
    status!("graph-node", "Starting graph-node");

    let mut graph_node_child_command = CONFIG.spawn_graph_node_with_args(&args).await?;

    let num_sources = aliases.len();

    let stream = tokio_stream::iter(cases_to_run)
        .enumerate()
        .map(|(index, case)| {
            let subgraph_name = format!("subgraph-{}", num_sources + index);
            case.check_health_and_test(&contracts, subgraph_name)
        })
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
