//! GraphQL assertion execution for test validation.
//!
//! After all mock blocks have been indexed, this module executes GraphQL
//! queries against the indexed data and compares results to expected values
//! from the test file.

use super::runner::TestContext;
use super::schema::{Assertion, AssertionFailure, TestResult};
use anyhow::{anyhow, Result};
use graph::data::query::{Query, QueryResults, QueryTarget};
use graph::prelude::{q, r, ApiVersion, GraphQlRunner as GraphQlRunnerTrait};

/// Run all GraphQL assertions from the test file.
///
/// Each assertion is a GraphQL query + expected JSON result. Returns `Passed`
/// if all assertions match, or `Failed` with the list of mismatches.
pub(super) async fn run_assertions(
    ctx: &TestContext,
    assertions: &[Assertion],
) -> Result<TestResult> {
    let mut failures = Vec::new();

    for assertion in assertions {
        match run_single_assertion(ctx, assertion).await {
            Ok(None) => {} // Passed
            Ok(Some(failure)) => failures.push(failure),
            Err(e) => {
                // Query execution error — record as a failure with the error message.
                failures.push(AssertionFailure {
                    query: assertion.query.clone(),
                    expected: assertion.expected.clone(),
                    actual: serde_json::json!({ "error": e.to_string() }),
                });
            }
        }
    }

    if failures.is_empty() {
        Ok(TestResult::Passed)
    } else {
        Ok(TestResult::Failed {
            handler_error: None,
            assertion_failures: failures,
        })
    }
}

/// Execute a single GraphQL assertion and compare the result.
///
/// Returns `None` if the assertion passed (actual == expected),
/// or `Some(AssertionFailure)` with the diff.
async fn run_single_assertion(
    ctx: &TestContext,
    assertion: &Assertion,
) -> Result<Option<AssertionFailure>> {
    // Query targets the specific deployment (not by subgraph name).
    let target = QueryTarget::Deployment(ctx.deployment.hash.clone(), ApiVersion::default());
    let query = Query::new(
        q::parse_query(&assertion.query)
            .map_err(|e| anyhow!("Failed to parse query: {:?}", e))?
            .into_static(),
        None,
        false,
    );

    let query_res: QueryResults = ctx.graphql_runner.clone().run_query(query, target).await;

    let result = query_res
        .first()
        .ok_or_else(|| anyhow!("No query result"))?
        .duplicate()
        .to_result()
        .map_err(|errors| anyhow!("Query errors: {:?}", errors))?;

    // Convert graph-node's internal r::Value to serde_json::Value for comparison.
    let actual_json = match result {
        Some(value) => r_value_to_json(&value),
        None => serde_json::Value::Null,
    };

    if json_equal(&actual_json, &assertion.expected) {
        Ok(None)
    } else {
        Ok(Some(AssertionFailure {
            query: assertion.query.clone(),
            expected: assertion.expected.clone(),
            actual: actual_json,
        }))
    }
}

/// Convert graph-node's internal `r::Value` (GraphQL result) to `serde_json::Value`.
///
/// Graph-node uses its own value type for GraphQL results. This converts to
/// standard JSON for comparison with the expected values in the test file.
fn r_value_to_json(value: &r::Value) -> serde_json::Value {
    match value {
        r::Value::Null => serde_json::Value::Null,
        r::Value::Boolean(b) => serde_json::Value::Bool(*b),
        r::Value::Int(n) => serde_json::Value::Number((*n).into()),
        r::Value::Float(f) => serde_json::json!(*f),
        r::Value::String(s) => serde_json::Value::String(s.clone()),
        r::Value::Enum(s) => serde_json::Value::String(s.clone()),
        r::Value::List(list) => {
            serde_json::Value::Array(list.iter().map(r_value_to_json).collect())
        }
        r::Value::Object(obj) => {
            let map: serde_json::Map<String, serde_json::Value> = obj
                .iter()
                .map(|(k, v)| (k.to_string(), r_value_to_json(v)))
                .collect();
            serde_json::Value::Object(map)
        }
        r::Value::Timestamp(t) => serde_json::Value::String(t.to_string()),
    }
}

/// Compare two JSON values for equality (ignoring key ordering in objects).
///
/// Also handles string-vs-number coercion: GraphQL returns `BigInt` and
/// `BigDecimal` fields as JSON strings (e.g., `"1000000000000000000"`),
/// but test authors may write them as JSON numbers. This function treats
/// `String("123")` and `Number(123)` as equal when they represent the
/// same value.
fn json_equal(a: &serde_json::Value, b: &serde_json::Value) -> bool {
    match (a, b) {
        (serde_json::Value::Null, serde_json::Value::Null) => true,
        (serde_json::Value::Bool(a), serde_json::Value::Bool(b)) => a == b,
        (serde_json::Value::Number(a), serde_json::Value::Number(b)) => a == b,
        (serde_json::Value::String(a), serde_json::Value::String(b)) => a == b,
        // String-vs-number coercion for BigInt/BigDecimal fields.
        (serde_json::Value::String(s), serde_json::Value::Number(n))
        | (serde_json::Value::Number(n), serde_json::Value::String(s)) => s == &n.to_string(),
        (serde_json::Value::Array(a), serde_json::Value::Array(b)) => {
            a.len() == b.len() && a.iter().zip(b.iter()).all(|(a, b)| json_equal(a, b))
        }
        (serde_json::Value::Object(a), serde_json::Value::Object(b)) => {
            a.len() == b.len()
                && a.iter()
                    .all(|(k, v)| b.get(k).map(|bv| json_equal(v, bv)).unwrap_or(false))
        }
        _ => false,
    }
}
