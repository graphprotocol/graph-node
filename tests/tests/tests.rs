use std::env;
use std::fs;
use std::process::Command;

#[test]
fn overloaded_contract_functions() {
    let cwd = env::current_dir().unwrap();
    let test_dir = cwd.join("overloaded-contract-functions");

    assert!(Command::new("yarn")
        .current_dir(test_dir.clone())
        .status()
        .expect("Command `yarn` failed to run")
        .success());

    let graph = test_dir.join("node_modules/.bin/graph");
    let graph_node = fs::canonicalize("../target/debug/graph-node").unwrap();

    assert!(Command::new(graph)
        .args(&[
            "test",
            "--standalone-node",
            graph_node.to_str().unwrap(),
            "yarn test"
        ])
        .current_dir(test_dir)
        .status()
        .expect("Command `graph test` failed to run")
        .success());
}
