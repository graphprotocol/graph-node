use std::collections::HashMap;

use graph::prelude::reqwest;

use serde::{Deserialize, Serialize};

/// Query variables for the remote GraphQL endpoint during debugging.
#[derive(Serialize)]
pub struct QueryVariables {
    pub id: Option<String>,
}

fn build_query(
    query: &'static str,
    variables: QueryVariables,
) -> graphql_client::QueryBody<QueryVariables> {
    graphql_client::QueryBody {
        variables,
        query,
        operation_name: "DebugQuery",
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ResponseData {
    // Oof, how to change this one?
    pub gravatars: Vec<HashMap<String, serde_json::Value>>,
}

pub fn perform_query(
    query: &'static str,
    variables: QueryVariables,
) -> Result<String, anyhow::Error> {
    println!("{}", "=".repeat(100));
    println!("IN PERFORM QUERY");
    println!("{}", "=".repeat(100));

    let request_body = build_query(query, variables);

    let client = reqwest::blocking::Client::new();
    let res = client
        .post(
            "https://api.thegraph.com/subgraphs/id/QmfEiYDc9ZvueQrvezFQy4EBQDcqbu74EY3oLWYmA7aAZq",
        )
        .json(&request_body)
        .send()?;

    let response_body: String = res.text()?;
    Ok(response_body)
}
