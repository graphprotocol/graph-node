pub mod util;

use std::time::Duration;

use graph::prelude::DeploymentHash;
use serde::Deserialize;
use serde_json::json;
use test_store::create_test_subgraph;
use tokio::time::sleep;

use self::util::client::send_graphql_request;
use self::util::run_test;
use self::util::server::VALID_TOKEN;

const TEST_SUBGRAPH_SCHEMA: &str = "type User @entity { id: ID!, name: String }";

async fn assert_deployment_paused(hash: &str, should_be_paused: bool) {
    let query = r#"query DeploymentStatus($hash: String!) {
        deployment {
            info(deployment: { hash: $hash }) {
                status {
                    isPaused
                }
            }
        }
    }"#;

    let resp = send_graphql_request(
        json!({
            "query": query,
            "variables": {
                "hash": hash
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
                        "status": {
                            "isPaused": should_be_paused
                        }
                    }
                ]
            }
        }
    });

    assert_eq!(resp, expected_resp);
}

#[test]
fn graphql_can_pause_deployments() {
    run_test(|| async {
        let deployment_hash = DeploymentHash::new("subgraph_1").unwrap();
        create_test_subgraph(&deployment_hash, TEST_SUBGRAPH_SCHEMA).await;

        let deployment_hash = DeploymentHash::new("subgraph_2").unwrap();
        create_test_subgraph(&deployment_hash, TEST_SUBGRAPH_SCHEMA).await;

        let resp = send_graphql_request(
            json!({
                "query": r#"mutation {
                    deployment {
                        pause(deployment: { hash: "subgraph_2" }) {
                            success
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
                    "pause": {
                        "success": true,
                    }
                }
            }
        });

        assert_eq!(resp, expected_resp);

        assert_deployment_paused("subgraph_2", true).await;
        assert_deployment_paused("subgraph_1", false).await;
    });
}

#[test]
fn graphql_can_resume_deployments() {
    run_test(|| async {
        let deployment_hash = DeploymentHash::new("subgraph_1").unwrap();
        create_test_subgraph(&deployment_hash, TEST_SUBGRAPH_SCHEMA).await;

        send_graphql_request(
            json!({
                "query": r#"mutation {
                    deployment {
                        pause(deployment: { hash: "subgraph_1" }) {
                            success
                        }
                    }
                }"#
            }),
            VALID_TOKEN,
        )
        .await;

        assert_deployment_paused("subgraph_1", true).await;

        send_graphql_request(
            json!({
                "query": r#"mutation {
                    deployment {
                        resume(deployment: { hash: "subgraph_1" }) {
                            success
                        }
                    }
                }"#
            }),
            VALID_TOKEN,
        )
        .await;

        assert_deployment_paused("subgraph_1", false).await;
    });
}

#[test]
fn graphql_can_restart_deployments() {
    run_test(|| async {
        let deployment_hash = DeploymentHash::new("subgraph_1").unwrap();
        create_test_subgraph(&deployment_hash, TEST_SUBGRAPH_SCHEMA).await;

        let deployment_hash = DeploymentHash::new("subgraph_2").unwrap();
        create_test_subgraph(&deployment_hash, TEST_SUBGRAPH_SCHEMA).await;

        send_graphql_request(
            json!({
                "query": r#"mutation {
                    deployment {
                        restart(deployment: { hash: "subgraph_2" }, delaySeconds: 2)
                    }
                }"#
            }),
            VALID_TOKEN,
        )
        .await;

        assert_deployment_paused("subgraph_2", true).await;
        assert_deployment_paused("subgraph_1", false).await;

        sleep(Duration::from_secs(5)).await;

        assert_deployment_paused("subgraph_2", false).await;
        assert_deployment_paused("subgraph_1", false).await;
    });
}

#[test]
fn graphql_allows_tracking_restart_deployment_executions() {
    run_test(|| async {
        let deployment_hash = DeploymentHash::new("subgraph_1").unwrap();
        create_test_subgraph(&deployment_hash, TEST_SUBGRAPH_SCHEMA).await;

        let resp = send_graphql_request(
            json!({
                "query": r#"mutation {
                    deployment {
                        restart(deployment: { hash: "subgraph_1" }, delaySeconds: 2)
                    }
                }"#
            }),
            VALID_TOKEN,
        )
        .await;

        #[derive(Deserialize)]
        struct Response {
            data: Data,
        }

        #[derive(Deserialize)]
        struct Data {
            deployment: Deployment,
        }

        #[derive(Deserialize)]
        struct Deployment {
            restart: String,
        }

        let resp: Response = serde_json::from_value(resp).expect("response is valid");
        let execution_id = resp.data.deployment.restart;

        let query = r#"query TrackRestartDeployment($id: String!) {
            execution {
                info(id: $id) {
                    id
                    kind
                    status
                    errorMessage
                }
            }
        }"#;

        let resp = send_graphql_request(
            json!({
                "query": query,
                "variables": {
                    "id": execution_id
                }
            }),
            VALID_TOKEN,
        )
        .await;

        let expected_resp = json!({
            "data": {
                "execution": {
                    "info": {
                        "id": execution_id,
                        "kind": "RESTART_DEPLOYMENT",
                        "status": "RUNNING",
                        "errorMessage": null,
                    }
                }
            }
        });

        assert_eq!(resp, expected_resp);

        sleep(Duration::from_secs(5)).await;

        let resp = send_graphql_request(
            json!({
                "query": query,
                "variables": {
                    "id": execution_id
                }
            }),
            VALID_TOKEN,
        )
        .await;

        let expected_resp = json!({
            "data": {
                "execution": {
                    "info": {
                        "id": execution_id,
                        "kind": "RESTART_DEPLOYMENT",
                        "status": "SUCCEEDED",
                        "errorMessage": null,
                    }
                }
            }
        });

        assert_eq!(resp, expected_resp);
    });
}

#[test]
fn graphql_can_create_new_subgraph() {
    run_test(|| async {
        let resp = send_graphql_request(
            json!({
                "query": r#"mutation CreateSubgraph {
                    deployment {
                        create(name: "subgraph_1") {
                            success
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
                    "create": {
                        "success": true,
                    }
                }
            }
        });

        assert_eq!(resp, expected_resp);
    });
}

#[test]
fn graphql_cannot_create_new_subgraph_with_invalid_name() {
    run_test(|| async {
        let resp = send_graphql_request(
            json!({
                "query": r#"mutation CreateInvalidSubgraph {
                    deployment {
                        create(name: "*@$%^subgraph") {
                            success
                        }
                    }
                }"#
            }),
            VALID_TOKEN,
        )
        .await;

        let success_resp = json!({
            "data": {
                "deployment": {
                    "create": {
                        "success": true,
                    }
                }
            }
        });

        assert_ne!(resp, success_resp);
    });
}

#[test]
fn graphql_can_remove_subgraph() {
    run_test(|| async {
        let resp = send_graphql_request(
            json!({
                "query": r#"mutation RemoveSubgraph {
                    deployment {
                        remove(name: "subgraph_1") {
                            success
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
                    "remove": {
                        "success": true,
                    }
                }
            }
        });

        assert_eq!(resp, expected_resp);
    });
}

#[test]
fn graphql_cannot_remove_subgraph_with_invalid_name() {
    run_test(|| async {
        let resp = send_graphql_request(
            json!({
                "query": r#"mutation RemoveInvalidSubgraph {
                    deployment {
                        remove(name: "*@$%^subgraph") {
                            success
                        }
                    }
                }"#
            }),
            VALID_TOKEN,
        )
        .await;

        let success_resp = json!({
            "data": {
                "deployment": {
                    "remove": {
                        "success": true,
                    }
                }
            }
        });

        assert_ne!(resp, success_resp);
    });
}

#[test]
fn graphql_can_unassign_deployments() {
    run_test(|| async {
        let deployment_hash = DeploymentHash::new("subgraph_1").unwrap();
        create_test_subgraph(&deployment_hash, TEST_SUBGRAPH_SCHEMA).await;

        let unassign_req = send_graphql_request(
            json!({
                "query": r#"mutation {
                    deployment {
                        unassign(deployment: { hash: "subgraph_1" }){
                            success
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
                    "unassign": {
                        "success": true,
                    }
                }
            }
        });

        let subgraph_node_id = send_graphql_request(
            json!({
                "query": r#"{
                    deployment {
                        info(deployment: { hash: "subgraph_1" }) {
                            nodeId
                        }
                    }
                }"#
            }),
            VALID_TOKEN,
        )
        .await;

        let is_node_null = subgraph_node_id["data"]["deployment"]["info"][0]["nodeId"].is_null();

        assert_eq!(unassign_req, expected_resp);
        assert_eq!(is_node_null, true);
    });
}

#[test]
fn graphql_cannot_unassign_deployments_twice() {
    run_test(|| async {
        let deployment_hash = DeploymentHash::new("subgraph_1").unwrap();
        create_test_subgraph(&deployment_hash, TEST_SUBGRAPH_SCHEMA).await;

        send_graphql_request(
            json!({
                "query": r#"mutation {
                    deployment {
                        unassign(deployment: { hash: "subgraph_1" }){
                            success
                        }
                    }
                }"#
            }),
            VALID_TOKEN,
        )
        .await;

        let unassign_again = send_graphql_request(
            json!({
                "query": r#"mutation {
                    deployment {
                        unassign(deployment: { hash: "subgraph_1" }){
                            success
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
                    "unassign": {
                        "success": true,
                    }
                }
            }
        });

        assert_ne!(unassign_again, expected_resp);
    });
}

#[test]
fn graphql_can_reassign_deployment() {
    run_test(|| async {
        let deployment_hash = DeploymentHash::new("subgraph_1").unwrap();
        create_test_subgraph(&deployment_hash, TEST_SUBGRAPH_SCHEMA).await;

        let deployment_hash = DeploymentHash::new("subgraph_2").unwrap();
        create_test_subgraph(&deployment_hash, TEST_SUBGRAPH_SCHEMA).await;

        send_graphql_request(
            json!({
                "query": r#"mutation {
                    deployment {
                        unassign(deployment: { hash: "subgraph_1" }){
                            success
                        }
                    }
                }"#
            }),
            VALID_TOKEN,
        )
        .await;

        let reassign = send_graphql_request(
            json!({
                "query": r#"mutation {
                    deployment {
                        reassign(deployment: { hash: "subgraph_1" }, node: "test") {
                            ... on EmptyResponse {
                                success
                            }
                            ... on CompletedWithWarnings {
                                warnings
                            }
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
                    "reassign": {
                        "success": true,
                    }
                }
            }
        });

        assert_eq!(reassign, expected_resp);
    });
}

#[test]
fn graphql_warns_reassign_on_wrong_node_id() {
    run_test(|| async {
        let deployment_hash = DeploymentHash::new("subgraph_1").unwrap();
        create_test_subgraph(&deployment_hash, TEST_SUBGRAPH_SCHEMA).await;

        let reassign = send_graphql_request(
            json!({
                "query": r#"mutation {
                    deployment {
                        reassign(deployment: { hash: "subgraph_1" }, node: "invalid_node") {
                            ... on EmptyResponse {
                                success
                            }
                            ... on CompletedWithWarnings {
                                warnings
                            }
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
                    "reassign": {
                        "warnings": ["This is the only deployment assigned to 'invalid_node'. Please make sure that the node ID is spelled correctly."],
                    }
                }
            }
        });

        assert_eq!(reassign, expected_resp);
    });
}
