use std::collections::HashMap;

use anyhow::anyhow;
use graph::futures03::StreamExt;
use graph_tests::config::set_dev_mode;
use graph_tests::contract::Contract;
use graph_tests::integration::{stop_graph_node, TestCase, TestResult};
use graph_tests::integration_cases::{
    subgraph_data_sources, test_block_handlers, test_multiple_subgraph_datasources,
};
use graph_tests::subgraph::Subgraph;
use graph_tests::{error, status, CONFIG};

/// The main test entrypoint.
#[graph::test]
async fn gnd_tests() -> anyhow::Result<()> {
    set_dev_mode(true);

    let test_name_to_run = std::env::var("TEST_CASE").ok();

    // 1. Deploy contracts and reset database
    let contracts = Contract::deploy_all().await?;

    status!("setup", "Resetting database");
    CONFIG.reset_database();

    // 2. Build source subgraphs: patch -> codegen -> build+upload -> capture hash
    status!("setup", "Building source subgraphs");
    let source_names = ["source-subgraph", "source-subgraph-a", "source-subgraph-b"];
    let mut source_hashes: HashMap<String, String> = HashMap::new();
    for source_name in &source_names {
        let dir = Subgraph::dir(source_name);
        Subgraph::patch(&dir, &contracts).await?;
        Subgraph::codegen_dev(&dir, false).await?;
        let hash = Subgraph::build_dev(&dir, true)
            .await?
            .expect("build --ipfs must return hash");
        status!("setup", "Built {source_name} -> {hash}");
        source_hashes.insert(source_name.to_string(), hash);
    }

    // 3. Create test cases with dynamic hashes from the build step
    let cases = vec![
        TestCase::new("block-handlers", test_block_handlers),
        TestCase::new_with_source_subgraphs(
            "subgraph-data-sources",
            subgraph_data_sources,
            vec![&format!(
                "{}:source-subgraph",
                source_hashes["source-subgraph"]
            )],
        ),
        TestCase::new_with_source_subgraphs(
            "multiple-subgraph-datasources",
            test_multiple_subgraph_datasources,
            vec![
                &format!("{}:source-subgraph-a", source_hashes["source-subgraph-a"]),
                &format!("{}:source-subgraph-b", source_hashes["source-subgraph-b"]),
            ],
        ),
    ];

    // 4. Filter test cases
    let cases_to_run: Vec<_> = if let Some(test_name) = test_name_to_run {
        cases
            .into_iter()
            .filter(|case| case.name == test_name)
            .collect()
    } else {
        cases
    };

    // 5. Prepare and build each main subgraph
    for case in &cases_to_run {
        case.prepare(&contracts).await?;
        let dir = Subgraph::dir(&case.name);
        let has_subgraph_ds = case.source_subgraph.is_some();
        Subgraph::codegen_dev(&dir, has_subgraph_ds).await?;
        Subgraph::build_dev(&dir, false).await?;
    }
    status!("setup", "Prepared all cases");

    // 6. Collect manifests and source aliases for gnd dev
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

    // 7. Start gnd dev
    status!("graph-node", "Starting graph-node");

    let mut graph_node_child_command = CONFIG.spawn_graph_node_with_args(&args).await?;

    let num_sources = aliases.len();

    let stream = tokio_stream::iter(cases_to_run)
        .enumerate()
        .map(|(index, case)| {
            let subgraph_name = format!("subgraph-{}", num_sources + index);
            case.check_health_and_test(subgraph_name)
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
