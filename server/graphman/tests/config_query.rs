pub mod util;

use graph::log::logger;
use graphman::config::Config;
use serde_json::json;

use self::util::client::send_graphql_request;
use self::util::run_test;
use self::util::server::VALID_TOKEN;

#[test]
fn graphql_can_validate_config_and_subgraphs_settings_config() {
    run_test(|| async {
        let curr_dir = std::env::current_dir()
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        let config_dir = format!("{}/tests/test_config.toml", curr_dir);
        let subgraph_settings_dir = format!("{}/tests/test_subgraph_settings.toml", curr_dir);
        std::env::set_var("GRAPH_NODE_CONFIG", config_dir);
        std::env::set_var(
            "GRAPH_EXPERIMENTAL_SUBGRAPH_SETTINGS",
            subgraph_settings_dir,
        );

        let resp = send_graphql_request(
            json!({
                "query": r#"{
                    config {
                        check {
                            configValidated
                            subgraphSettingsValidated
                        }
                    }
                }"#
            }),
            VALID_TOKEN,
        )
        .await;

        let expected_resp = json!({
            "data": {
                "config": {
                    "check": {
                        "configValidated": true,
                        "subgraphSettingsValidated": true
                    }
                }
            }
        });

        assert_eq!(resp, expected_resp);
    });
}

#[test]
fn graphql_can_return_config_as_json_string() {
    run_test(|| async {
        let curr_dir = std::env::current_dir()
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        let config_dir = format!("{}/tests/test_config.toml", curr_dir);
        std::env::set_var("GRAPH_NODE_CONFIG", config_dir.clone());

        let resp = send_graphql_request(
            json!({
                "query": r#"{
                    config {
                        check {
                            config
                        }
                    }
                }"#
            }),
            VALID_TOKEN,
        )
        .await;

        let config = Config::from_file(&logger(true), &config_dir, "default")
            .unwrap()
            .to_json()
            .unwrap();

        assert_eq!(
            resp.get("data")
                .unwrap()
                .get("config")
                .unwrap()
                .get("check")
                .unwrap()
                .get("config")
                .unwrap()
                .as_str()
                .unwrap(),
            config
        );
    });
}

#[test]
fn graphql_can_check_how_specific_subgraph_would_be_placed() {
    run_test(|| async {
        let curr_dir = std::env::current_dir()
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        let config_dir = format!("{}/tests/test_config.toml", curr_dir);
        std::env::set_var("GRAPH_NODE_CONFIG", config_dir);

        let resp = send_graphql_request(
            json!({
                "query": r#"{
                    config {
                        place(subgraph: "subgraph_1", network: "bsc") {
                            subgraph
                            network
                            shards
                            nodes
                        }
                    }
                }"#
            }),
            VALID_TOKEN,
        )
        .await;

        let expected_resp = json!({
            "data": {
                "config": {
                    "place": {
                        "subgraph": String::from("subgraph_1"),
                        "network": String::from("bsc"),
                        "shards": vec!["primary".to_string(),"shard_a".to_string()],
                        "nodes": vec!["index_node_1_a".to_string(),"index_node_2_a".to_string(),"index_node_3_a".to_string()],
                    }
                }
            }
        });

        let resp_custom = send_graphql_request(
            json!({
                "query": r#"{
                    config {
                        place(subgraph: "custom/subgraph_1", network: "bsc") {
                            subgraph
                            network
                            shards
                            nodes
                        }
                    }
                }"#
            }),
            VALID_TOKEN,
        )
        .await;

        let expected_resp_custom = json!({
            "data": {
                "config": {
                    "place": {
                        "subgraph": String::from("custom/subgraph_1"),
                        "network": String::from("bsc"),
                        "shards": vec!["primary".to_string()],
                        "nodes": vec!["index_custom_0".to_string()],
                    }
                }
            }
        });

        assert_eq!(resp, expected_resp);
        assert_eq!(resp_custom, expected_resp_custom);
    });
}

#[test]
fn graphql_can_fetch_info_about_size_of_database_pools() {
    run_test(|| async {
        let curr_dir = std::env::current_dir()
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        let config_dir = format!("{}/tests/test_config.toml", curr_dir);
        std::env::set_var("GRAPH_NODE_CONFIG", config_dir);

        let resp = send_graphql_request(
            json!({
                "query": r#"{
                    config {
                        pools(nodes: ["index_node_1_a", "index_node_2_a", "index_node_3_a"]) {
                            pools {
                                shards
                                node
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
                "config": {
                    "pools": {
                        "pools": [
                            {
                                "shards": {
                                    "primary": 2,
                                    "shard_a": 2
                                },
                                "node": "index_node_1_a"
                            },
                            {
                                "shards": {
                                    "primary": 10,
                                    "shard_a": 10
                                },
                                "node": "index_node_2_a"
                            },
                            {
                                "shards": {
                                    "primary": 10,
                                    "shard_a": 10
                                },
                                "node": "index_node_3_a"
                            }
                        ]
                    }
                }
            }
        });

        assert_eq!(resp, expected_resp);
    });
}

#[test]
fn graphql_can_fetch_connections_by_shard() {
    run_test(|| async {
        let curr_dir = std::env::current_dir()
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        let config_dir = format!("{}/tests/test_config.toml", curr_dir);
        std::env::set_var("GRAPH_NODE_CONFIG", config_dir);

        let resp = send_graphql_request(
            json!({
                "query": r#"{
                    config {
                        pools(nodes: ["index_node_1_a", "index_node_2_a", "index_node_3_a"]) {
                            shards
                        }
                    }
                }"#
            }),
            VALID_TOKEN,
        )
        .await;

        let expected_resp = json!({
            "data": {
                "config": {
                    "pools": {
                        "shards": {
                            "primary": 22,
                            "shard_a": 22
                        }
                    }
                }
            }
        });

        assert_eq!(resp, expected_resp);
    });
}
