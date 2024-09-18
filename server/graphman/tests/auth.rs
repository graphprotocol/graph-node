pub mod util;

use serde_json::json;

use self::util::client::send_graphql_request;
use self::util::client::send_request;
use self::util::client::BASE_URL;
use self::util::client::CLIENT;
use self::util::run_test;
use self::util::server::INVALID_TOKEN;
use self::util::server::VALID_TOKEN;

#[test]
fn graphql_playground_is_accessible() {
    run_test(|| async {
        send_request(CLIENT.head(BASE_URL.as_str())).await;
    });
}

#[test]
fn graphql_requests_are_not_allowed_without_a_valid_token() {
    run_test(|| async {
        let resp = send_graphql_request(
            json!({
                "query": "{ __typename }"
            }),
            INVALID_TOKEN,
        )
        .await;

        let expected_resp = json!({
            "errors": [
                {
                    "message": "You are not authorized to access this resource",
                    "extensions": {
                        "code": "UNAUTHORIZED"
                    }
                }
            ],
            "data": null
        });

        assert_eq!(resp, expected_resp);
    });
}

#[test]
fn graphql_requests_are_allowed_with_a_valid_token() {
    run_test(|| async {
        let resp = send_graphql_request(
            json!({
                "query": "{ __typename }"
            }),
            VALID_TOKEN,
        )
        .await;

        let expected_resp = json!({
            "data": {
                "__typename": "QueryRoot"
            }
        });

        assert_eq!(resp, expected_resp);
    });
}
