use bollard::image::CreateImageOptions;
use bollard::models::HostConfig;
use bollard::{container, Docker};
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use std::collections::HashMap;
use tokio::time::{sleep, Duration};

use crate::helpers::{contains_subslice, MappedPorts};

type DockerError = bollard::errors::Error;

const POSTGRES_IMAGE: &str = "postgres:latest";
const IPFS_IMAGE: &str = "ipfs/go-ipfs:v0.10.0";
const GANACHE_IMAGE: &str = "trufflesuite/ganache-cli:latest";

pub async fn pull_service_images() {
    let client =
        Docker::connect_with_local_defaults().expect("Failed to connect to docker daemon.");

    let image_names = [POSTGRES_IMAGE, IPFS_IMAGE, GANACHE_IMAGE];

    image_names
        .iter()
        .map(|image_name| pull_image(&client, image_name))
        // .await them in no specific order.
        .collect::<FuturesUnordered<_>>()
        .collect::<Vec<_>>()
        .await;
}

async fn pull_image(client: &Docker, image_name: &str) {
    let options = Some(CreateImageOptions {
        from_image: image_name,
        ..Default::default()
    });

    let mut stream = client.create_image(options, None, None);

    while let Some(res) = stream.next().await {
        if let Err(err) = res {
            panic!("Error when pulling docker image `{}`: {}", image_name, err)
        }
    }
}

pub async fn kill_and_remove(client: &Docker, container_name: &str) -> Result<(), DockerError> {
    client.kill_container::<&str>(container_name, None).await?;
    client.remove_container(container_name, None).await
}

/// Represents all possible service containers to be spawned
#[derive(Debug, Copy, Clone)]
pub enum ServiceContainerKind {
    Postgres,
    Ipfs,
    Ganache,
}

impl ServiceContainerKind {
    fn config(&self) -> container::Config<&'static str> {
        let host_config = HostConfig {
            publish_all_ports: Some(true),
            ..Default::default()
        };

        match self {
            Self::Postgres => container::Config {
                image: Some(POSTGRES_IMAGE),
                env: Some(vec![
                    "POSTGRES_PASSWORD=password",
                    "POSTGRES_USER=postgres",
                    "POSTGRES_INITDB_ARGS=-E UTF8 --locale=C",
                ]),
                host_config: Some(host_config),
                cmd: Some(vec![
                    "postgres",
                    "-N",
                    "1000",
                    "-cshared_preload_libraries=pg_stat_statements",
                ]),
                ..Default::default()
            },

            Self::Ipfs => container::Config {
                image: Some(IPFS_IMAGE),
                host_config: Some(host_config),
                ..Default::default()
            },

            Self::Ganache => container::Config {
                image: Some(GANACHE_IMAGE),
                cmd: Some(vec!["-d", "-l", "100000000000", "-g", "1"]),
                host_config: Some(host_config),
                ..Default::default()
            },
        }
    }

    pub fn name(&self) -> &str {
        use ServiceContainerKind::*;
        match self {
            Postgres => "graph_node_integration_test_postgres",
            Ipfs => "graph_node_integration_test_ipfs",
            Ganache => "graph_node_integration_test_ganache",
        }
    }
}

/// Handles the connection to the docker daemon and keeps track the service running inside it.
pub struct ServiceContainer {
    client: Docker,
    container_name: String,
}

impl ServiceContainer {
    pub async fn start(service: ServiceContainerKind) -> Result<Self, DockerError> {
        let client =
            Docker::connect_with_local_defaults().expect("Failed to connect to docker daemon.");
        let container_name =
            format!("{}-{}", service.name(), uuid::Uuid::new_v4()).replace('-', "_");

        let docker_test_client = Self {
            container_name: container_name.clone(),
            client,
        };

        docker_test_client
            .client
            .create_container(
                Some(container::CreateContainerOptions {
                    name: container_name.clone(),
                }),
                service.config(),
            )
            .await?;

        docker_test_client
            .client
            .start_container::<&'static str>(&container_name, None)
            .await?;

        Ok(docker_test_client)
    }

    pub fn container_name(&self) -> &str {
        &self.container_name
    }

    pub async fn stop(&self) -> Result<(), DockerError> {
        kill_and_remove(&self.client, self.container_name()).await
    }

    pub async fn exposed_ports(&self) -> Result<MappedPorts, DockerError> {
        use bollard::models::ContainerSummaryInner;

        let results = {
            let mut filters = HashMap::new();
            filters.insert("name".to_string(), vec![self.container_name().to_string()]);
            let options = Some(container::ListContainersOptions {
                filters,
                limit: Some(1),
                ..Default::default()
            });
            self.client.list_containers(options).await?
        };

        let ports = match &results.as_slice() {
            &[ContainerSummaryInner {
                ports: Some(ports), ..
            }] => ports,
            unexpected_response => panic!(
                "Received a unexpected_response from docker API: {:#?}",
                unexpected_response
            ),
        };

        Ok(to_mapped_ports(ports.clone()))
    }

    /// halts execution until a trigger message is detected on stdout or, optionally,
    /// waits for a specified amount of time after the message appears.
    pub async fn wait_for_message(
        &self,
        trigger_message: &[u8],
        hard_wait: Duration,
    ) -> Result<&Self, DockerError> {
        // listen to container logs
        let mut stream = self.client.logs::<String>(
            &self.container_name,
            Some(container::LogsOptions {
                follow: true,
                stdout: true,
                stderr: true,
                ..Default::default()
            }),
        );

        // halt execution until a message is received
        loop {
            match stream.next().await {
                Some(Ok(log_line)) => {
                    if contains_subslice(log_line.to_string().as_bytes(), trigger_message) {
                        break;
                    }
                }
                Some(Err(error)) => return Err(error),
                None => panic!("stream ended before expected message could be detected"),
            }
        }

        sleep(hard_wait).await;
        Ok(self)
    }

    /// Calls `docker exec` on the container to create a test database.
    pub async fn create_postgres_database(
        docker: &ServiceContainer,
        db_name: &str,
    ) -> Result<(), DockerError> {
        const EXEC_TRIES: usize = 10;

        use bollard::exec;

        for attempt in 1..=EXEC_TRIES {
            // 1. Create Exec
            let config = exec::CreateExecOptions {
                cmd: Some(vec!["createdb", "-E", "UTF8", "--locale=C", db_name]),
                user: Some("postgres"),
                attach_stdout: Some(true),
                ..Default::default()
            };

            let message = docker
                .client
                .create_exec(docker.container_name(), config)
                .await?;

            // 2. Start Exec
            let mut stream = docker.client.start_exec(&message.id, None);
            while let Some(_) = stream.next().await { /* consume stream */ }

            // 3. Inspect exec
            let inspect = docker.client.inspect_exec(&message.id).await?;
            match inspect.exit_code {
                Some(0) => return Ok(()),
                code if attempt < EXEC_TRIES => {
                    println!(
                        "failed to run 'createdb' (exit code: {:?}) Will try again, attempt {attempt} of {EXEC_TRIES}",
                        code
                    );
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
                code => panic!(
                    "failed to run 'createdb' command using docker exec (exit code: {:?})",
                    code
                ),
            }
        }
        Ok(())
    }
}

fn to_mapped_ports(input: Vec<bollard::models::Port>) -> MappedPorts {
    let mut hashmap = HashMap::new();

    for port in &input {
        if let bollard::models::Port {
            private_port,
            public_port: Some(public_port),
            ..
        } = port
        {
            hashmap.insert(*private_port as u16, *public_port as u16);
        }
    }
    if hashmap.is_empty() {
        panic!("Container exposed no ports. Input={:?}", input)
    }
    hashmap
}
