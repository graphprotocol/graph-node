use graph::http::header::AUTHORIZATION;
use lazy_static::lazy_static;
use reqwest::Client;
use reqwest::RequestBuilder;
use reqwest::Response;
use serde_json::Value;

use crate::util::server::PORT;

lazy_static! {
    pub static ref CLIENT: Client = Client::new();
    pub static ref BASE_URL: String = format!("http://127.0.0.1:{PORT}");
}

pub async fn send_request(req: RequestBuilder) -> Response {
    req.send()
        .await
        .expect("server is accessible")
        .error_for_status()
        .expect("response status is OK")
}

pub async fn send_graphql_request(data: Value, token: &str) -> Value {
    send_request(
        CLIENT
            .post(BASE_URL.as_str())
            .json(&data)
            .header(AUTHORIZATION, format!("Bearer {token}")),
    )
    .await
    .json()
    .await
    .expect("GraphQL response is valid JSON")
}
