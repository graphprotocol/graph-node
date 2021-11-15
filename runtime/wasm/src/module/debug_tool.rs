use std::sync::Arc;

use graph::prelude::{reqwest, Schema};
use graph_graphql::graphql_parser::schema;

use serde::Serialize;

/// Just a wrapper around the GraphQL query string.
#[derive(Serialize)]
pub struct Query {
    query: String,
    variables: Variables,
}

#[derive(Serialize)]
pub struct Variables {
    id: String,
}

fn from_template(entity_type: &str, fields: &Vec<String>) -> String {
    format!(
        "\
query Query ($id: String) {{
    {}(id: $id, subgraphError: allow) {{
        id
        {}
    }}
}}",
        entity_type,
        format!("\n{}\n", fields.join("\n")),
    )
}

pub fn infer_query(schema: Arc<Schema>, entity_type: &str, id: &str) -> (Query, Vec<String>) {
    let fields: Vec<String> = schema
        .document
        .definitions
        .iter()
        .find_map(|def| {
            if let schema::Definition::TypeDefinition(schema::TypeDefinition::Object(o)) = def {
                if o.name == entity_type {
                    Some(o)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .unwrap()
        .fields
        .iter()
        .map(|field| field.name.to_string())
        .collect();

    let query = Query {
        query: from_template(&entity_type.to_lowercase(), &fields),
        variables: Variables { id: id.to_string() },
    };
    return (query, fields);
}

pub fn perform_query(query: Query) -> Result<String, anyhow::Error> {
    let client = reqwest::blocking::Client::new();
    let res = client
        .post(
            "https://api.thegraph.com/subgraphs/id/QmfEiYDc9ZvueQrvezFQy4EBQDcqbu74EY3oLWYmA7aAZq",
        )
        .json(&query)
        .send()?;

    let res = res.text()?;
    Ok(res)
}
