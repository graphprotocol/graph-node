use crate::{
    fixture::{stores, Stores, TestInfo},
    helpers::run_cmd,
};
use graph::ipfs;
use graph::prelude::{DeploymentHash, SubgraphName};
use std::process::Command;
pub struct RunnerTestRecipe {
    pub stores: Stores,
    pub test_info: TestInfo,
}

impl RunnerTestRecipe {
    pub async fn new(test_name: &str, subgraph_name: &str) -> Self {
        let subgraph_name = SubgraphName::new(subgraph_name).unwrap();
        let test_dir = format!("./runner-tests/{}", subgraph_name);

        let (stores, hash) = tokio::join!(
            stores(test_name, "./runner-tests/config.simple.toml"),
            build_subgraph(&test_dir, None)
        );

        Self {
            stores,
            test_info: TestInfo {
                test_dir,
                test_name: test_name.to_string(),
                subgraph_name,
                hash,
            },
        }
    }

    /// Builds a new test subgraph with a custom deploy command.
    pub async fn new_with_custom_cmd(name: &str, subgraph_name: &str, deploy_cmd: &str) -> Self {
        let subgraph_name = SubgraphName::new(subgraph_name).unwrap();
        let test_dir = format!("./runner-tests/{}", subgraph_name);

        let (stores, hash) = tokio::join!(
            stores(name, "./runner-tests/config.simple.toml"),
            build_subgraph(&test_dir, Some(deploy_cmd))
        );

        Self {
            stores,
            test_info: TestInfo {
                test_dir,
                test_name: name.to_string(),
                subgraph_name,
                hash,
            },
        }
    }

    pub async fn new_with_file_link_resolver(
        name: &str,
        subgraph_name: &str,
        manifest: &str,
    ) -> Self {
        let subgraph_name = SubgraphName::new(subgraph_name).unwrap();
        let test_dir = format!("./runner-tests/{}", subgraph_name);

        let stores = stores(name, "./runner-tests/config.simple.toml").await;
        build_subgraph(&test_dir, None).await;
        let hash = DeploymentHash::new(manifest).unwrap();
        Self {
            stores,
            test_info: TestInfo {
                test_dir,
                test_name: name.to_string(),
                subgraph_name,
                hash,
            },
        }
    }
}

/// deploy_cmd is the command to run to deploy the subgraph. If it is None, the
/// default `yarn deploy:test` is used.
async fn build_subgraph(dir: &str, deploy_cmd: Option<&str>) -> DeploymentHash {
    build_subgraph_with_yarn_cmd(dir, deploy_cmd.unwrap_or("deploy:test")).await
}

async fn build_subgraph_with_yarn_cmd(dir: &str, yarn_cmd: &str) -> DeploymentHash {
    build_subgraph_with_yarn_cmd_and_arg(dir, yarn_cmd, None).await
}

pub async fn build_subgraph_with_yarn_cmd_and_arg(
    dir: &str,
    yarn_cmd: &str,
    arg: Option<&str>,
) -> DeploymentHash {
    // Test that IPFS is up.
    ipfs::IpfsRpcClient::new(ipfs::ServerAddress::local_rpc_api(), &graph::log::discard())
        .await
        .expect("Could not connect to IPFS, make sure it's running at port 5001");

    // Make sure dependencies are present.

    run_cmd(
        Command::new("yarn")
            .arg("install")
            .arg("--mutex")
            .arg("file:.yarn-mutex")
            .current_dir("./runner-tests/"),
    );

    // Run codegen.
    run_cmd(Command::new("yarn").arg("codegen").current_dir(dir));

    let mut args = vec![yarn_cmd];
    args.extend(arg);

    // Run `deploy` for the side effect of uploading to IPFS, the graph node url
    // is fake and the actual deploy call is meant to fail.
    let deploy_output = run_cmd(
        Command::new("yarn")
            .args(&args)
            .env("IPFS_URI", "http://127.0.0.1:5001")
            .env("GRAPH_NODE_ADMIN_URI", "http://localhost:0")
            .current_dir(dir),
    );

    // Hack to extract deployment id from `graph deploy` output.
    const ID_PREFIX: &str = "Build completed: ";
    let Some(mut line) = deploy_output.lines().find(|line| line.contains(ID_PREFIX)) else {
        panic!("No deployment id found, graph deploy probably had an error")
    };
    if !line.starts_with(ID_PREFIX) {
        line = &line[5..line.len() - 5]; // workaround for colored output
    }
    DeploymentHash::new(line.trim_start_matches(ID_PREFIX)).unwrap()
}
