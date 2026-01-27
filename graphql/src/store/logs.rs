use graph::components::log_store::LogQuery;
use graph::prelude::{q, r, DeploymentHash, QueryExecutionError};

use crate::execution::ast as a;

const MAX_FIRST: u32 = 1000;
const MAX_SKIP: u32 = 10000;
const MAX_TEXT_LENGTH: usize = 1000;

/// Validate and sanitize text search input to prevent injection attacks
fn validate_text_input(text: &str) -> Result<(), &'static str> {
    if text.is_empty() {
        return Err("search text cannot be empty");
    }

    if text.len() > MAX_TEXT_LENGTH {
        return Err("search text exceeds maximum length of 1000 characters");
    }

    // Reject strings that look like Elasticsearch query DSL to prevent injection
    if text
        .chars()
        .any(|c| matches!(c, '{' | '}' | '[' | ']' | ':' | '"'))
    {
        return Err("search text contains invalid characters ({}[]:\")");
    }

    Ok(())
}

/// Validate RFC3339 timestamp format
fn validate_timestamp(timestamp: &str) -> Result<(), &'static str> {
    if !timestamp.contains('T') {
        return Err("must be in ISO 8601 format (e.g., 2024-01-15T10:30:00Z)");
    }

    if !timestamp.ends_with('Z') && !timestamp.contains('+') && !timestamp.contains('-') {
        return Err("must include timezone (Z or offset like +00:00)");
    }

    if timestamp.len() > 50 {
        return Err("timestamp exceeds maximum length");
    }

    // Check for suspicious characters that could be injection attempts
    if timestamp
        .chars()
        .any(|c| matches!(c, '{' | '}' | ';' | '\'' | '"'))
    {
        return Err("timestamp contains invalid characters");
    }

    Ok(())
}

pub fn build_log_query(
    field: &a::Field,
    subgraph_id: &DeploymentHash,
) -> Result<LogQuery, QueryExecutionError> {
    let mut level = None;
    let mut from = None;
    let mut to = None;
    let mut search = None;
    let mut first = 100;
    let mut skip = 0;
    let mut order_direction = graph::components::log_store::OrderDirection::Desc;

    // Parse arguments
    for (name, value) in &field.arguments {
        match name.as_str() {
            "level" => {
                if let r::Value::Enum(level_str) = value {
                    level = Some(level_str.parse().map_err(|e: String| {
                        QueryExecutionError::InvalidArgumentError(
                            field.position,
                            "level".to_string(),
                            q::Value::String(e),
                        )
                    })?);
                }
            }
            "from" => {
                if let r::Value::String(from_str) = value {
                    validate_timestamp(from_str).map_err(|e| {
                        QueryExecutionError::InvalidArgumentError(
                            field.position,
                            "from".to_string(),
                            q::Value::String(format!("Invalid timestamp: {}", e)),
                        )
                    })?;
                    from = Some(from_str.clone());
                }
            }
            "to" => {
                if let r::Value::String(to_str) = value {
                    validate_timestamp(to_str).map_err(|e| {
                        QueryExecutionError::InvalidArgumentError(
                            field.position,
                            "to".to_string(),
                            q::Value::String(format!("Invalid timestamp: {}", e)),
                        )
                    })?;
                    to = Some(to_str.clone());
                }
            }
            "search" => {
                if let r::Value::String(search_str) = value {
                    validate_text_input(search_str).map_err(|e| {
                        QueryExecutionError::InvalidArgumentError(
                            field.position,
                            "search".to_string(),
                            q::Value::String(format!("Invalid search text: {}", e)),
                        )
                    })?;
                    search = Some(search_str.clone());
                }
            }
            "first" => {
                if let r::Value::Int(first_val) = value {
                    let first_i64 = *first_val;
                    if first_i64 < 0 {
                        return Err(QueryExecutionError::InvalidArgumentError(
                            field.position,
                            "first".to_string(),
                            q::Value::String("first must be non-negative".to_string()),
                        ));
                    }
                    let first_u32 = first_i64 as u32;
                    if first_u32 > MAX_FIRST {
                        return Err(QueryExecutionError::InvalidArgumentError(
                            field.position,
                            "first".to_string(),
                            q::Value::String(format!("first must not exceed {}", MAX_FIRST)),
                        ));
                    }
                    first = first_u32;
                }
            }
            "skip" => {
                if let r::Value::Int(skip_val) = value {
                    let skip_i64 = *skip_val;
                    if skip_i64 < 0 {
                        return Err(QueryExecutionError::InvalidArgumentError(
                            field.position,
                            "skip".to_string(),
                            q::Value::String("skip must be non-negative".to_string()),
                        ));
                    }
                    let skip_u32 = skip_i64 as u32;
                    if skip_u32 > MAX_SKIP {
                        return Err(QueryExecutionError::InvalidArgumentError(
                            field.position,
                            "skip".to_string(),
                            q::Value::String(format!("skip must not exceed {}", MAX_SKIP)),
                        ));
                    }
                    skip = skip_u32;
                }
            }
            "orderDirection" => {
                if let r::Value::Enum(order_str) = value {
                    order_direction = order_str.parse().map_err(|e: String| {
                        QueryExecutionError::InvalidArgumentError(
                            field.position,
                            "orderDirection".to_string(),
                            q::Value::String(e),
                        )
                    })?;
                }
            }
            _ => {
                // Unknown argument, ignore
            }
        }
    }

    Ok(LogQuery {
        subgraph_id: subgraph_id.clone(),
        level,
        from,
        to,
        search,
        first,
        skip,
        order_direction,
    })
}
