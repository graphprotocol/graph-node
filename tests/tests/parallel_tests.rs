mod common;
use anyhow::Context;
use common::docker::{pull_images, DockerTestClient, TestContainerService};
use common::helpers::{
    basename, get_unique_ganache_counter, get_unique_postgres_counter, make_ganache_uri,
    make_ipfs_uri, make_postgres_uri, pretty_output, GraphNodePorts, MappedPorts,
};
use futures::StreamExt;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::process::{Child, Command};

const DEFAULT_N_CONCURRENT_TESTS: usize = 15;

lazy_static::lazy_static! {
    static ref GANACHE_HARD_WAIT_SECONDS: Option<u64> =
        parse_numeric_environment_variable("TESTS_GANACHE_HARD_WAIT_SECONDS");
    static ref IPFS_HARD_WAIT_SECONDS: Option<u64> =
        parse_numeric_environment_variable("TESTS_IPFS_HARD_WAIT_SECONDS");
    static ref POSTGRES_HARD_WAIT_SECONDS: Option<u64> =
        parse_numeric_environment_variable("TESTS_POSTGRES_HARD_WAIT_SECONDS");
}

/// All integration tests subdirectories to run
pub const INTEGRATION_TESTS_DIRECTORIES: [&str; 11] = [
    "api-version-v0-0-4",
    "data-source-context",
    "data-source-revert",
    "fatal-error",
    "ganache-reverts",
    "host-exports",
    "non-fatal-errors",
    "overloaded-contract-functions",
    "poi-for-failed-subgraph",
    "remove-then-update",
    "value-roundtrip",
];

/// Contains all information a test command needs
#[derive(Debug)]
struct IntegrationTestSetup {
    postgres_uri: String,
    ipfs_uri: String,
    ganache_port: u16,
    ganache_uri: String,
    graph_node_ports: GraphNodePorts,
    graph_node_bin: Arc<PathBuf>,
    test_directory: PathBuf,
}

impl IntegrationTestSetup {
    fn test_name(&self) -> String {
        basename(&self.test_directory)
    }

    fn graph_node_admin_uri(&self) -> String {
        let ws_port = self.graph_node_ports.admin;
        format!("http://localhost:{}/", ws_port)
    }
}

/// Info about a finished test command
#[derive(Debug)]
struct TestCommandResults {
    success: bool,
    _exit_code: Option<i32>,
    stdout: String,
    stderr: String,
}

#[derive(Debug)]
struct StdIO {
    stdout: Option<String>,
    stderr: Option<String>,
}
impl std::fmt::Display for StdIO {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(ref stdout) = self.stdout {
            write!(f, "{}", stdout)?;
        }
        if let Some(ref stderr) = self.stderr {
            write!(f, "{}", stderr)?
        }
        Ok(())
    }
}

// The results of a finished integration test
#[derive(Debug)]
struct IntegrationTestResult {
    test_setup: IntegrationTestSetup,
    test_command_results: TestCommandResults,
    graph_node_stdio: StdIO,
}

impl IntegrationTestResult {
    fn print_outcome(&self) {
        let status = match self.test_command_results.success {
            true => "SUCCESS",
            false => "FAILURE",
        };
        println!("- Test: {}: {}", status, self.test_setup.test_name())
    }

    fn print_failure(&self) {
        if self.test_command_results.success {
            return;
        }
        let test_name = self.test_setup.test_name();
        println!("=============");
        println!("\nFailed test: {}", test_name);
        println!("-------------");
        println!("{:#?}", self.test_setup);
        println!("-------------");
        println!("\nFailed test command output:");
        println!("---------------------------");
        println!("{}", self.test_command_results.stdout);
        println!("{}", self.test_command_results.stderr);
        println!("--------------------------");
        println!("graph-node command output:");
        println!("--------------------------");
        println!("{}", self.graph_node_stdio);
    }
}

/// The main test entrypoint
#[tokio::test]
async fn parallel_integration_tests() -> anyhow::Result<()> {
    // use a environment variable for limiting the number of concurrent tests
    let n_parallel_tests: usize = std::env::var("N_CONCURRENT_TESTS")
        .ok()
        .and_then(|x| x.parse().ok())
        .unwrap_or(DEFAULT_N_CONCURRENT_TESTS);

    let current_working_directory =
        std::env::current_dir().context("failed to identify working directory")?;
    let integration_tests_root_directory = current_working_directory.join("integration-tests");

    // pull required docker images
    pull_images().await;

    let test_directories = INTEGRATION_TESTS_DIRECTORIES
        .iter()
        .map(|ref p| integration_tests_root_directory.join(PathBuf::from(p)))
        .collect::<Vec<PathBuf>>();

    // Show discovered tests
    println!("Found {} integration tests:", test_directories.len());
    for dir in &test_directories {
        println!("  - {}", basename(dir));
    }

    // run `yarn` command to build workspace
    run_yarn_command(&integration_tests_root_directory).await;

    // start docker containers for Postgres and IPFS and wait for them to be ready
    let postgres = Arc::new(
        DockerTestClient::start(TestContainerService::Postgres)
            .await
            .context("failed to start container service for Postgres.")?,
    );
    postgres
        .wait_for_message(
            b"database system is ready to accept connections",
            &*POSTGRES_HARD_WAIT_SECONDS,
        )
        .await
        .context("failed to wait for Postgres container to be ready to accept connections")?;

    let ipfs = DockerTestClient::start(TestContainerService::Ipfs)
        .await
        .context("failed to start container service for IPFS.")?;
    ipfs.wait_for_message(b"Daemon is ready", &*IPFS_HARD_WAIT_SECONDS)
        .await
        .context("failed to wait for Ipfs container to be ready to accept connections")?;

    let postgres_ports = Arc::new(
        postgres
            .exposed_ports()
            .await
            .context("failed to obtain exposed ports for the Postgres container")?,
    );
    let ipfs_ports = Arc::new(
        ipfs.exposed_ports()
            .await
            .context("failed to obtain exposed ports for the IPFS container")?,
    );

    let graph_node = Arc::new(
        fs::canonicalize("../target/debug/graph-node")
            .context("failed to infer `graph-node` program location. (Was it built already?)")?,
    );

    // run tests
    let mut test_results = Vec::new();

    let mut stream = tokio_stream::iter(test_directories)
        .map(|dir| {
            run_integration_test(
                dir.clone(),
                postgres.clone(),
                postgres_ports.clone(),
                ipfs_ports.clone(),
                graph_node.clone(),
            )
        })
        .buffered(n_parallel_tests);

    let mut failed = false;
    while let Some(test_result) = stream.next().await {
        let test_result = test_result?;
        if !test_result.test_command_results.success {
            failed = true;
        }
        test_results.push(test_result);
    }

    // Stop containers.
    postgres
        .stop()
        .await
        .context("failed to stop container service for Postgres")?;
    ipfs.stop()
        .await
        .context("failed to stop container service for IPFS")?;

    // print failures
    for failed_test in test_results
        .iter()
        .filter(|t| !t.test_command_results.success)
    {
        failed_test.print_failure()
    }

    // print test result summary
    println!("\nTest results:");
    for test_result in &test_results {
        test_result.print_outcome()
    }

    if failed {
        Err(anyhow::anyhow!("Some tests have failed"))
    } else {
        Ok(())
    }
}

/// Prepare and run the integration test
async fn run_integration_test(
    test_directory: PathBuf,
    postgres_docker: Arc<DockerTestClient>,
    postgres_ports: Arc<MappedPorts>,
    ipfs_ports: Arc<MappedPorts>,
    graph_node_bin: Arc<PathBuf>,
) -> anyhow::Result<IntegrationTestResult> {
    // start a dedicated ganache container for this test
    let unique_ganache_counter = get_unique_ganache_counter();
    let ganache = DockerTestClient::start(TestContainerService::Ganache(unique_ganache_counter))
        .await
        .context("failed to start container service for Ganache.")?;
    ganache
        .wait_for_message(b"Listening on ", &*GANACHE_HARD_WAIT_SECONDS)
        .await
        .context("failed to wait for Ganache container to be ready to accept connections")?;

    let ganache_ports: MappedPorts = ganache
        .exposed_ports()
        .await
        .context("failed to obtain exposed ports for Ganache container")?;

    // build URIs
    let postgres_unique_id = get_unique_postgres_counter();

    let postgres_uri = make_postgres_uri(&postgres_unique_id, &postgres_ports);
    let ipfs_uri = make_ipfs_uri(&ipfs_ports);
    let (ganache_port, ganache_uri) = make_ganache_uri(&ganache_ports);

    // create test database
    DockerTestClient::create_postgres_database(&postgres_docker, &postgres_unique_id)
        .await
        .context("failed to create the test database.")?;

    // prepare to run test comand
    let test_setup = IntegrationTestSetup {
        postgres_uri,
        ipfs_uri,
        ganache_uri,
        ganache_port,
        graph_node_bin,
        graph_node_ports: GraphNodePorts::get_ports(),
        test_directory,
    };

    // spawn graph-node
    let mut graph_node_child_command = run_graph_node(&test_setup).await?;

    println!("Test started: {}", basename(&test_setup.test_directory));
    let test_command_results = run_test_command(&test_setup).await?;

    // stop graph-node

    let graph_node_stdio = stop_graph_node(&mut graph_node_child_command).await?;
    // stop ganache container
    ganache
        .stop()
        .await
        .context("failed to stop container service for Ganache")?;

    Ok(IntegrationTestResult {
        test_setup,
        test_command_results,
        graph_node_stdio,
    })
}

/// Runs a command for a integration test
async fn run_test_command(test_setup: &IntegrationTestSetup) -> anyhow::Result<TestCommandResults> {
    let output = Command::new("yarn")
        .arg("test")
        .env("GANACHE_TEST_PORT", test_setup.ganache_port.to_string())
        .env("GRAPH_NODE_ADMIN_URI", test_setup.graph_node_admin_uri())
        .env(
            "GRAPH_NODE_HTTP_PORT",
            test_setup.graph_node_ports.http.to_string(),
        )
        .env(
            "GRAPH_NODE_INDEX_PORT",
            test_setup.graph_node_ports.index.to_string(),
        )
        .env("IPFS_URI", &test_setup.ipfs_uri)
        .current_dir(&test_setup.test_directory)
        .output()
        .await
        .context("failed to run test command")?;

    let test_name = test_setup.test_name();
    let stdout_tag = format!("[{}:stdout] ", test_name);
    let stderr_tag = format!("[{}:stderr] ", test_name);

    Ok(TestCommandResults {
        success: output.status.success(),
        _exit_code: output.status.code(),
        stdout: pretty_output(&output.stdout, &stdout_tag),
        stderr: pretty_output(&output.stderr, &stderr_tag),
    })
}
async fn run_graph_node(test_setup: &IntegrationTestSetup) -> anyhow::Result<Child> {
    use std::process::Stdio;

    let mut command = Command::new(test_setup.graph_node_bin.as_os_str());
    command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        // postgres
        .arg("--postgres-url")
        .arg(&test_setup.postgres_uri)
        // ethereum
        .arg("--ethereum-rpc")
        .arg(&test_setup.ganache_uri)
        // ipfs
        .arg("--ipfs")
        .arg(&test_setup.ipfs_uri)
        // http port
        .arg("--http-port")
        .arg(test_setup.graph_node_ports.http.to_string())
        // index node port
        .arg("--index-node-port")
        .arg(test_setup.graph_node_ports.index.to_string())
        // ws  port
        .arg("--ws-port")
        .arg(test_setup.graph_node_ports.ws.to_string())
        // admin  port
        .arg("--admin-port")
        .arg(test_setup.graph_node_ports.admin.to_string())
        // metrics  port
        .arg("--metrics-port")
        .arg(test_setup.graph_node_ports.metrics.to_string());

    // add test specific environment variables
    // TODO: it might be interesting to refactor this conditional into a new datatype that ties
    // the test name and its environment variables together.
    if test_setup.test_name().as_str() == "data-source-revert" {
        command.env(
            "FAILPOINTS",
            "test_reorg=return(2);error_on_duplicate_ds=return",
        );
    }

    // This is necessary because most integration tests finish before the default threshold (3
    // minutes).
    command.env("GRAPH_SUBGRAPH_SYNC_STATUS_UPDATE_THRESHOLD_MILLIS", "50");

    command
        .spawn()
        .context("failed to start graph-node command.")
}

async fn stop_graph_node(child: &mut Child) -> anyhow::Result<StdIO> {
    child.kill().await.context("Failed to kill graph-node")?;

    // capture stdio
    let stdout = match child.stdout.take() {
        Some(mut data) => Some(process_stdio(&mut data, "[graph-node:stdout] ").await?),
        None => None,
    };
    let stderr = match child.stderr.take() {
        Some(mut data) => Some(process_stdio(&mut data, "[graph-node:stderr] ").await?),
        None => None,
    };

    Ok(StdIO { stdout, stderr })
}

async fn process_stdio<T: AsyncReadExt + Unpin>(
    stdio: &mut T,
    prefix: &str,
) -> anyhow::Result<String> {
    let mut buffer: Vec<u8> = Vec::new();
    stdio
        .read_to_end(&mut buffer)
        .await
        .context("failed to read stdio")?;
    Ok(pretty_output(&buffer, prefix))
}

/// run yarn to build everything
async fn run_yarn_command(base_directory: &impl AsRef<Path>) {
    println!("Running `yarn` command in integration tests root directory.");
    let output = Command::new("yarn")
        .current_dir(base_directory)
        .output()
        .await
        .expect("failed to run yarn command");

    if output.status.success() {
        return;
    }
    println!("Yarn command failed.");
    println!("{}", pretty_output(&output.stdout, "[yarn:stdout]"));
    println!("{}", pretty_output(&output.stderr, "[yarn:stderr]"));
    panic!("Yarn command failed.")
}

fn parse_numeric_environment_variable(environment_variable_name: &str) -> Option<u64> {
    std::env::var(environment_variable_name)
        .ok()
        .and_then(|x| x.parse().ok())
}
