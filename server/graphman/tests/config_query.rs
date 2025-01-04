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
