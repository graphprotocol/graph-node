use crate::common::helpers::{contains_subslice, postgres_test_database_name, MappedPorts};
use bollard::image::CreateImageOptions;
use bollard::models::HostConfig;
use bollard::{container, Docker};
use std::collections::HashMap;
use tokio::time::{sleep, Duration};
use tokio_stream::StreamExt;

const POSTGRES_IMAGE: &'static str = "postgres:latest";
const IPFS_IMAGE: &'static str = "ipfs/go-ipfs:v0.4.23";
const GANACHE_IMAGE: &'static str = "trufflesuite/ganache-cli:latest";
type DockerError = bollard::errors::Error;

pub async fn pull_images() {
    use tokio_stream::StreamMap;

    let client =
        Docker::connect_with_local_defaults().expect("Failed to connect to docker daemon.");

    let images = [POSTGRES_IMAGE, IPFS_IMAGE, GANACHE_IMAGE];
    let mut map = StreamMap::new();

    for image_name in &images {
        let options = Some(CreateImageOptions {
            from_image: *image_name,
            ..Default::default()
        });
        let stream = client.create_image(options, None, None);
        map.insert(*image_name, stream);
    }

    while let Some(message) = map.next().await {
        if let (key, Err(msg)) = message {
            panic!("Error when pulling docker image for {}: {}", key, msg)
        }
    }
}

pub async fn stop_and_remove(client: &Docker, service_name: &str) -> Result<(), DockerError> {
    client.kill_container::<&str>(service_name, None).await?;
    client.remove_container(service_name, None).await
}

/// Represents all possible service containers to be spawned
#[derive(Debug)]
pub enum TestContainerService {
    Postgres,
    Ipfs,
    Ganache(u16),
}

impl TestContainerService {
    fn config(&self) -> container::Config<&'static str> {
        use TestContainerService::*;
        match self {
            Postgres => Self::build_postgres_container_config(),
            Ipfs => Self::build_ipfs_container_config(),
            Ganache(_u32) => Self::build_ganache_container_config(),
        }
    }

    fn options(&self) -> container::CreateContainerOptions<String> {
        container::CreateContainerOptions { name: self.name() }
    }

    fn name(&self) -> String {
        use TestContainerService::*;
        match self {
            Postgres => "graph_node_integration_test_postgres".into(),
            Ipfs => "graph_node_integration_test_ipfs".into(),
            Ganache(container_count) => {
                format!("graph_node_integration_test_ganache_{}", container_count)
            }
        }
    }

    fn build_postgres_container_config() -> container::Config<&'static str> {
        let host_config = HostConfig {
            publish_all_ports: Some(true),
            ..Default::default()
        };

        container::Config {
            image: Some(POSTGRES_IMAGE),
            env: Some(vec!["POSTGRES_PASSWORD=password", "POSTGRES_USER=postgres"]),
            host_config: Some(host_config),
            cmd: Some(vec![
                "postgres",
                "-N",
                "1000",
                "-cshared_preload_libraries=pg_stat_statements",
            ]),
            ..Default::default()
        }
    }

    fn build_ipfs_container_config() -> container::Config<&'static str> {
        let host_config = HostConfig {
            publish_all_ports: Some(true),
            ..Default::default()
        };

        container::Config {
            image: Some(IPFS_IMAGE),
            host_config: Some(host_config),
            ..Default::default()
        }
    }

    fn build_ganache_container_config() -> container::Config<&'static str> {
        let host_config = HostConfig {
            publish_all_ports: Some(true),
            ..Default::default()
        };

        container::Config {
            image: Some(GANACHE_IMAGE),
            cmd: Some(vec!["-d", "-l", "100000000000", "-g", "1"]),
            host_config: Some(host_config),
            ..Default::default()
        }
    }
}

/// Handles the connection to the docker daemon and keeps track the service running inside it.
pub struct DockerTestClient {
    service: TestContainerService,
    client: Docker,
}

impl DockerTestClient {
    pub async fn start(service: TestContainerService) -> Result<Self, DockerError> {
        let client =
            Docker::connect_with_local_defaults().expect("Failed to connect to docker daemon.");

        let docker_test_client = Self { service, client };

        // try to remove the container if it already exists
        let _ = stop_and_remove(
            &docker_test_client.client,
            &docker_test_client.service.name(),
        )
        .await;

        // create docker container
        docker_test_client
            .client
            .create_container(
                Some(docker_test_client.service.options()),
                docker_test_client.service.config(),
            )
            .await?;

        // start docker container
        docker_test_client
            .client
            .start_container::<&'static str>(&docker_test_client.service.name(), None)
            .await?;

        Ok(docker_test_client)
    }

    pub async fn stop(&self) -> Result<(), DockerError> {
        stop_and_remove(&self.client, &self.service.name()).await
    }

    pub async fn exposed_ports(&self) -> Result<MappedPorts, DockerError> {
        use bollard::models::ContainerSummaryInner;
        let mut filters = HashMap::new();
        filters.insert("name".to_string(), vec![self.service.name()]);
        let options = Some(container::ListContainersOptions {
            filters,
            limit: Some(1),
            ..Default::default()
        });
        let results = self.client.list_containers(options).await?;
        let ports = match &results.as_slice() {
            &[ContainerSummaryInner {
                ports: Some(ports), ..
            }] => ports,
            unexpected_response => panic!(
                "Received a unexpected_response from docker API: {:#?}",
                unexpected_response
            ),
        };
        let mapped_ports: MappedPorts = ports.to_vec().into();
        Ok(mapped_ports)
    }

    /// halts execution until a trigger message is detected on stdout
    pub async fn wait_for_message(&self, trigger_message: &[u8]) -> Result<&Self, DockerError> {
        // listen to container logs
        let mut stream = self.client.logs::<String>(
            &self.service.name(),
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
                Some(Ok(container::LogOutput::StdOut { message })) => {
                    if contains_subslice(&message, &trigger_message) {
                        break Ok(self);
                    } else {
                        sleep(Duration::from_millis(100)).await;
                    }
                }
                Some(Err(error)) => break Err(error),
                None => {
                    panic!("stream ended before expected message could be detected")
                }
                _ => {}
            }
        }
    }

    /// Calls `docker exec` on the container to create a test database.
    pub async fn create_postgres_database(
        docker: &DockerTestClient,
        unique_id: &u16,
    ) -> Result<(), DockerError> {
        use bollard::exec;

        let database_name = postgres_test_database_name(unique_id);

        // 1. Create Exec
        let config = exec::CreateExecOptions {
            cmd: Some(vec!["createdb", &database_name]),
            user: Some("postgres"),
            attach_stdout: Some(true),
            ..Default::default()
        };

        let message = docker
            .client
            .create_exec(&docker.service.name(), config)
            .await?;

        // 2. Start Exec
        let mut stream = docker.client.start_exec(&message.id, None);
        while let Some(_) = stream.next().await { /* consume stream */ }

        // 3. Inspecet exec
        let inspect = docker.client.inspect_exec(&message.id).await?;
        if let Some(0) = inspect.exit_code {
            Ok(())
        } else {
            panic!("failed to run 'createdb' command using docker exec");
        }
    }
}

impl From<Vec<bollard::models::Port>> for MappedPorts {
    fn from(input: Vec<bollard::models::Port>) -> Self {
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
        MappedPorts(hashmap)
    }
}
