pub mod util;

use graph::data::subgraph::DeploymentHash;
use serde_json::json;
use test_store::store::create_test_subgraph;
use test_store::store::NETWORK_NAME;
use test_store::store::NODE_ID;

use self::util::client::send_graphql_request;
use self::util::run_test;
use self::util::server::VALID_TOKEN;

const TEST_SUBGRAPH_SCHEMA: &str = "type User @entity { id: ID!, name: String }";

#[test]
fn graphql_returns_deployment_info() {
    run_test(|| async {
        let deployment_hash = DeploymentHash::new("subgraph_1").unwrap();
        let locator = create_test_subgraph(&deployment_hash, TEST_SUBGRAPH_SCHEMA).await;

        let resp = send_graphql_request(
            json!({
                "query": r#"{
                    deployment {
                        info {
                            hash
                            namespace
                            name
                            nodeId
                            shard
                            chain
                            versionStatus
                            isActive
                            status {
                                isPaused
                                isSynced
                                health
                                earliestBlockNumber
                                latestBlock {
                                    hash
                                    number
                                }
                                chainHeadBlock {
                                    hash
                                    number
                                }
                            }
                        }
                    }
                }"#
            }),
            VALID_TOKEN,
        )
        .await;

        let namespace = format!("sgd{}", locator.id);

        let expected_resp = json!({
            "data": {
                "deployment": {
                    "info": [
                        {
                            "hash": "subgraph_1",
                            "namespace": namespace,
                            "name": "subgraph_1",
                            "nodeId": NODE_ID.to_string(),
                            "shard": "primary",
                            "chain": NETWORK_NAME,
                            "versionStatus": "current",
                            "isActive": true,
                            "status": {
                                "isPaused": false,
                                "isSynced": false,
                                "health": "HEALTHY",
                                "earliestBlockNumber": "0",
                                "latestBlock": null,
                                "chainHeadBlock": null
                            }
                        }
                    ]
                }
            }
        });

        assert_eq!(resp, expected_resp);
    });
}

#[test]
fn graphql_returns_deployment_info_by_deployment_name() {
    run_test(|| async {
        let deployment_hash = DeploymentHash::new("subgraph_1").unwrap();
        create_test_subgraph(&deployment_hash, TEST_SUBGRAPH_SCHEMA).await;

        let deployment_hash = DeploymentHash::new("subgraph_2").unwrap();
        create_test_subgraph(&deployment_hash, TEST_SUBGRAPH_SCHEMA).await;

        let resp = send_graphql_request(
            json!({
                "query": r#"{
                    deployment {
                        info(deployment: { name: "subgraph_1" }) {
                            name
                        }
                    }
                }"#
            }),
            VALID_TOKEN,
        )
        .await;

        let expected_resp = json!({
            "data": {
                "deployment": {
                    "info": [
                        {
                            "name": "subgraph_1"
                        }
                    ]
                }
            }
        });

        assert_eq!(resp, expected_resp);
    });
}

#[test]
fn graphql_returns_deployment_info_by_deployment_hash() {
    run_test(|| async {
        let deployment_hash = DeploymentHash::new("subgraph_1").unwrap();
        create_test_subgraph(&deployment_hash, TEST_SUBGRAPH_SCHEMA).await;

        let deployment_hash = DeploymentHash::new("subgraph_2").unwrap();
        create_test_subgraph(&deployment_hash, TEST_SUBGRAPH_SCHEMA).await;

        let resp = send_graphql_request(
            json!({
                "query": r#"{
                    deployment {
                        info(deployment: { hash: "subgraph_2" }) {
                            hash
                        }
                    }
                }"#
            }),
            VALID_TOKEN,
        )
        .await;

        let expected_resp = json!({
            "data": {
                "deployment": {
                    "info": [
                        {
                            "hash": "subgraph_2"
                        }
                    ]
                }
            }
        });

        assert_eq!(resp, expected_resp);
    });
}

#[test]
fn graphql_returns_deployment_info_by_deployment_namespace() {
    run_test(|| async {
        let deployment_hash = DeploymentHash::new("subgraph_1").unwrap();
        create_test_subgraph(&deployment_hash, TEST_SUBGRAPH_SCHEMA).await;

        let deployment_hash = DeploymentHash::new("subgraph_2").unwrap();
        let locator = create_test_subgraph(&deployment_hash, TEST_SUBGRAPH_SCHEMA).await;

        let namespace = format!("sgd{}", locator.id);

        let resp = send_graphql_request(
            json!({
                "query": r#"query DeploymentInfo($namespace: String!) {
                    deployment {
                        info(deployment: { schema: $namespace }) {
                            namespace
                        }
                    }
                }"#,
                "variables": {
                    "namespace": namespace
                }
            }),
            VALID_TOKEN,
        )
        .await;

        let expected_resp = json!({
            "data": {
                "deployment": {
                    "info": [
                        {
                            "namespace": namespace
                        }
                    ]
                }
            }
        });

        assert_eq!(resp, expected_resp);
    });
}

#[test]
fn graphql_returns_empty_deployment_info_when_there_are_no_deployments() {
    run_test(|| async {
        let resp = send_graphql_request(
            json!({
                "query": r#"{
                    deployment {
                        info {
                            name
                        }
                    }
                }"#
            }),
            VALID_TOKEN,
        )
        .await;

        let expected_resp = json!({
            "data": {
                "deployment": {
                    "info": []
                }
            }
        });

        assert_eq!(resp, expected_resp);
    });
}
