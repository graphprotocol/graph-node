use std::{fs, path::PathBuf, process::ExitStatus, sync::Arc};
use tokio::process::Child;

use anyhow::Context;
use std::{thread, time};
use tests::{
    docker::pull_images, get_unique_ganache_counter, make_ganache_uri, make_ipfs_uri,
    make_postgres_uri, run_graph_node, stop_graph_node, DockerTestClient, GraphNodePorts,
    IntegrationTestSetup, TestContainerService,
};
use tokio::process::Command;

struct PerformanceTestingEnvrionment {
    postgres: DockerTestClient,
    ipfs: DockerTestClient,
    ganache: DockerTestClient,
    graph_node: Option<Child>,
}

struct Endpoints {
    postgres: String,
    ipfs: String,
    ganache: (u16, String),
}

impl PerformanceTestingEnvrionment {
    pub async fn create_and_start() -> anyhow::Result<PerformanceTestingEnvrionment> {
        pull_images().await;

        let postgres = DockerTestClient::start(TestContainerService::Postgres)
            .await
            .context("failed to start container service for Postgres.")?;

        let ipfs = DockerTestClient::start(TestContainerService::Ipfs)
            .await
            .context("failed to start container service for IPFS.")?;

        let unique_ganache_counter = get_unique_ganache_counter();
        let ganache =
            DockerTestClient::start(TestContainerService::Ganache(unique_ganache_counter))
                .await
                .context("failed to start container service for Ganache.")?;

        Ok(PerformanceTestingEnvrionment {
            postgres,
            ipfs,
            ganache,
            graph_node: None,
        })
    }

    async fn containers_ready(&self) -> anyhow::Result<()> {
        self.postgres
            .wait_for_message(b"database system is ready to accept connections", &Some(10))
            .await
            .context("failed to wait for Postgres container to be ready to accept connections")?;

        self.ipfs
            .wait_for_message(b"Daemon is ready", &Some(10))
            .await
            .context("failed to wait for Ipfs container to be ready to accept connections")?;

        self.ganache
            .wait_for_message(b"Listening on ", &Some(10))
            .await
            .context("failed to wait for Ganache container to be ready to accept connections")?;

        Ok(())
    }

    async fn endpoints(&self) -> anyhow::Result<Endpoints> {
        let postgres_ports = Arc::new(
            self.postgres
                .exposed_ports()
                .await
                .context("failed to obtain exposed ports for the Postgres container")?,
        );
        let ipfs_ports = Arc::new(
            self.ipfs
                .exposed_ports()
                .await
                .context("failed to obtain exposed ports for the IPFS container")?,
        );
        let ganache_ports = self
            .ganache
            .exposed_ports()
            .await
            .context("failed to obtain exposed ports for Ganache container")?;

        Ok(Endpoints {
            postgres: make_postgres_uri(&0, &postgres_ports),
            ipfs: make_ipfs_uri(&ipfs_ports),
            ganache: make_ganache_uri(&ganache_ports),
        })
    }

    async fn create_db(&self) -> anyhow::Result<()> {
        DockerTestClient::create_postgres_database(&self.postgres, &0)
            .await
            .context("failed to create the test database.")?;

        Ok(())
    }

    fn graph_node_bin_path(&self) -> PathBuf {
        fs::canonicalize("./target/debug/graph-node")
            .context("failed to infer `graph-node` program location. (Was it built already?)")
            .expect("failed to locate `graph-node` program")
    }

    async fn run_k6(&self, test_setup: &IntegrationTestSetup) -> anyhow::Result<ExitStatus> {
        let output = Command::new("k6")
            .arg("run")
            .arg("./k6-tests/k6.js")
            .env("GRAPHQL_PORT", test_setup.graph_node_ports.http.to_string())
            .output()
            .await
            .context("failed to run k6 command")?;

        println!(
            "run_k6: stdout: {}, stderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );

        Ok(output.status)
    }

    async fn deploy_and_seed(&mut self, test_setup: &IntegrationTestSetup) -> anyhow::Result<()> {
        let output = Command::new("yarn")
            .arg("prepare:env")
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
            .current_dir("./k6-tests/subgraph")
            .output()
            .await
            .context("failed to run test command")?;

        println!(
            "deploy_and_seed: stdout: {}",
            String::from_utf8_lossy(&output.stdout)
        );

        Ok(())
    }

    async fn spawn_graph_node(&mut self) -> anyhow::Result<IntegrationTestSetup> {
        let endpoints = self.endpoints().await?;
        let test_setup = IntegrationTestSetup {
            postgres_uri: endpoints.postgres,
            ipfs_uri: endpoints.ipfs,
            ganache_uri: endpoints.ganache.1,
            ganache_port: endpoints.ganache.0,
            graph_node_bin: Arc::new(self.graph_node_bin_path()),
            graph_node_ports: GraphNodePorts::get_ports(),
            test_directory: None,
        };

        println!("test_setup: {:?}", test_setup);

        self.graph_node = Some(run_graph_node(&test_setup).await?);

        Ok(test_setup)
    }

    async fn stop_graph_node(&mut self) -> anyhow::Result<()> {
        if let Some(graph_node_child) = &mut self.graph_node {
            let graph_node_stdio = stop_graph_node(graph_node_child).await?;
            println!("graph_node_stdio: {:?}", graph_node_stdio);
            self.graph_node = None;
        }

        Ok(())
    }

    async fn stop_containers(&self) -> anyhow::Result<()> {
        self.postgres
            .stop()
            .await
            .context("failed to stop container service for PostgreSQL")?;

        self.ipfs
            .stop()
            .await
            .context("failed to stop container service for IPFS")?;

        self.ganache
            .stop()
            .await
            .context("failed to stop container service for Ganache")?;

        Ok(())
    }
}

fn sleep(secs: u64) {
    thread::sleep(time::Duration::from_secs(secs));
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("Preparing to run query performance testing with K6...");
    let mut test_env = PerformanceTestingEnvrionment::create_and_start().await?;

    println!("Waiting for containers to be ready...");
    test_env.containers_ready().await?;

    println!("Creating databases...");
    test_env.create_db().await?;

    println!("Running graph-node in background...");
    let cfg = test_env.spawn_graph_node().await?;
    sleep(10);

    println!("Seeding Smart Contract and Subgraph...");
    test_env.deploy_and_seed(&cfg).await?;
    sleep(10);

    println!("Running K6 and executing performance testing....");
    let exit_code = test_env.run_k6(&cfg).await?;

    println!("Stopping graph-node");
    test_env.stop_graph_node().await?;

    println!("Stopping containers...");
    test_env.stop_containers().await?;

    std::process::exit(exit_code.code().unwrap_or(0));
}
