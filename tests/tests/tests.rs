use duct::cmd;
use lazy_static::lazy_static;
use std::env;
use std::fs;
use std::path::PathBuf;
use std::sync::Mutex;

fn test_dir(name: &str) -> PathBuf {
    let cwd = env::current_dir().expect("failed to identify working directory");
    cwd.join(name)
}

fn run_cmd(args: Vec<&str>, cwd: PathBuf) {
    let cmd_string = args.clone().join(" ");

    println!("running command: {}", cmd_string);

    let (program, args) = args.split_first().expect("empty command provided");

    let output = cmd(*program, args)
        .stderr_to_stdout()
        .stdout_capture()
        .dir(cwd)
        .unchecked()
        .run()
        .expect("failed to start command");

    let pretty_output = String::from_utf8(output.stdout)
        .unwrap()
        .trim()
        .split("\n")
        .map(|s| format!("│ {}", s))
        .collect::<Vec<_>>()
        .join("\n");

    print!("{}", pretty_output);

    if !output.status.success() {
        panic!("failed to run command `{}`", cmd_string);
    }
}

fn run_test(test: &str) {
    // Do not run integration tests for JSONB.
    if std::env::var("GRAPH_STORAGE_SCHEME") == Ok("json".to_string()) {
        return;
    }

    let dir = test_dir(&format!("{}{}", "integration-tests/", test));
    let graph = dir.join("node_modules/.bin/graph").clone();
    let graph_node = fs::canonicalize("../target/debug/graph-node").unwrap();

    run_cmd(vec!["yarn"], dir.clone());
    run_cmd(
        vec![
            graph.to_str().unwrap(),
            "test",
            "--standalone-node",
            graph_node.to_str().unwrap(),
            "--node-logs",
            "yarn test",
        ],
        dir,
    );
}

lazy_static! {
    // Necessary due to port conflict when running multiple nodes at once.
    // TODO: Allow tests to run in parallel.
    static ref TEST_MUTEX: Mutex<()> = Mutex::new(());
}

#[test]
fn overloaded_contract_functions() {
    let _m = TEST_MUTEX.lock();
    run_test("overloaded-contract-functions")
}

#[test]
fn data_source_context() {
    let _m = TEST_MUTEX.lock();
    run_test("data-source-context")
}

#[test]
fn arweave_and_3box() {
    let _m = TEST_MUTEX.lock();
    run_test("arweave-and-3box")
}

#[test]
fn ganache_reverts() {
    let _m = TEST_MUTEX.lock();
    run_test("ganache-reverts");
}

#[test]
fn fatal_error() {
    let _m = TEST_MUTEX.lock();
    run_test("fatal-error");
}

#[test]
fn remove_then_update() {
    let _m = TEST_MUTEX.lock();
    run_test("remove-then-update");
}

#[test]
fn value_roundtrip() {
    let _m = TEST_MUTEX.lock();
    run_test("value-roundtrip");
}

#[test]
fn big_decimal() {
    let _m = TEST_MUTEX.lock();
    run_test("big-decimal");
}
