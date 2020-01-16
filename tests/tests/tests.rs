extern crate duct;

use duct::cmd;
use std::env;
use std::fs;
use std::path::PathBuf;

fn test_dir(name: &str) -> PathBuf {
    let cwd = env::current_dir().expect("failed to identify working directory");
    cwd.join(name)
}

fn run_cmd(args: Vec<&str>, cwd: PathBuf) {
    let cmd_string = args.clone().join(" ");

    let (program, args) = args.split_first().expect("empty command provided");

    let output = cmd(*program, args)
        .stderr_to_stdout()
        .stdout_capture()
        .dir(cwd)
        .unchecked()
        .run()
        .expect("failed to start command");

    if !output.status.success() {
        panic!(format!(
            "Failed to run command `{}`:\n\n{}",
            cmd_string,
            String::from_utf8(output.stdout).unwrap()
        ));
    }
}

#[test]
fn overloaded_contract_functions() {
    let dir = test_dir("integration-tests/overloaded-contract-functions");
    let graph = dir.join("node_modules/.bin/graph").clone();
    let graph_node = fs::canonicalize("../target/debug/graph-node").unwrap();

    run_cmd(vec!["yarn"], dir.clone());
    run_cmd(
        vec![
            graph.to_str().unwrap(),
            "test",
            "--standalone-node",
            graph_node.to_str().unwrap(),
            "yarn test",
        ],
        dir,
    );
}
