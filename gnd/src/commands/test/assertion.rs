//! GraphQL assertion execution for test validation.

use super::runner::TestContext;
use super::schema::{Assertion, AssertionFailure, AssertionOutcome, TestResult};
use anyhow::{anyhow, Result};
use graph::data::query::{Query, QueryResults, QueryTarget};
use graph::prelude::{q, r, ApiVersion, GraphQlRunner as GraphQlRunnerTrait};

pub(super) async fn run_assertions(
    ctx: &TestContext,
    assertions: &[Assertion],
) -> Result<TestResult> {
    let mut outcomes = Vec::new();

    for assertion in assertions {
        match run_single_assertion(ctx, assertion).await {
            Ok(None) => {
                outcomes.push(AssertionOutcome::Passed {
                    query: assertion.query.clone(),
                });
            }
            Ok(Some(failure)) => {
                outcomes.push(AssertionOutcome::Failed(failure));
            }
            Err(e) => {
                outcomes.push(AssertionOutcome::Failed(AssertionFailure {
                    query: assertion.query.clone(),
                    expected: assertion.expected.clone(),
                    actual: serde_json::json!({ "error": e.to_string() }),
                }));
            }
        }
    }

    Ok(TestResult {
        handler_error: None,
        assertions: outcomes,
    })
}

/// Execute a single assertion. Returns `None` on pass, `Some(failure)` on mismatch.
async fn run_single_assertion(
    ctx: &TestContext,
    assertion: &Assertion,
) -> Result<Option<AssertionFailure>> {
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

/// Reorder `actual` arrays to align with `expected`'s element ordering.
///
/// When a test fails, the raw diff can be misleading if array elements appear
/// in a different order — every line shows as changed even if only one field
/// differs. This function reorders `actual` so that elements are paired with
/// their closest match in `expected`, producing a diff that highlights only
/// real value differences.
pub(super) fn align_for_diff(
    expected: &serde_json::Value,
    actual: &serde_json::Value,
) -> serde_json::Value {
    match (expected, actual) {
        (serde_json::Value::Array(exp), serde_json::Value::Array(act)) => {
            let mut used = vec![false; act.len()];
            let mut aligned = Vec::with_capacity(exp.len().max(act.len()));

            for exp_elem in exp {
                let best = act
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| !used[*i])
                    .max_by_key(|(_, a)| json_similarity(exp_elem, a));

                if let Some((idx, _)) = best {
                    used[idx] = true;
                    aligned.push(align_for_diff(exp_elem, &act[idx]));
                }
            }

            for (i, elem) in act.iter().enumerate() {
                if !used[i] {
                    aligned.push(elem.clone());
                }
            }

            serde_json::Value::Array(aligned)
        }
        (serde_json::Value::Object(exp), serde_json::Value::Object(act)) => {
            let aligned: serde_json::Map<String, serde_json::Value> = act
                .iter()
                .map(|(k, v)| {
                    let aligned_v = if let Some(exp_v) = exp.get(k) {
                        align_for_diff(exp_v, v)
                    } else {
                        v.clone()
                    };
                    (k.clone(), aligned_v)
                })
                .collect();
            serde_json::Value::Object(aligned)
        }
        _ => actual.clone(),
    }
}

/// Score how similar two JSON values are for use in [`align_for_diff`].
///
/// For objects, counts the number of fields whose values are equal in both.
/// A matching `"id"` field is weighted heavily (+100) since it is the
/// strongest signal that two objects represent the same entity.
/// For all other value types, returns 1 if equal, 0 otherwise.
fn json_similarity(a: &serde_json::Value, b: &serde_json::Value) -> usize {
    match (a, b) {
        (serde_json::Value::Object(a_obj), serde_json::Value::Object(b_obj)) => {
            let mut score = 0;
            for (k, v) in a_obj {
                if let Some(bv) = b_obj.get(k) {
                    if json_equal(v, bv) {
                        // `id` match is a strong signal for entity identity.
                        score += if k == "id" { 100 } else { 1 };
                    }
                }
            }
            score
        }
        _ => {
            if json_equal(a, b) {
                1
            } else {
                0
            }
        }
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
        (serde_json::Value::String(s), serde_json::Value::Number(n))
        | (serde_json::Value::Number(n), serde_json::Value::String(s)) => s == &n.to_string(),
        (serde_json::Value::Array(a), serde_json::Value::Array(b)) => {
            if a.len() != b.len() {
                return false;
            }
            // Order-insensitive: O(n²), fine for realistic test sizes.
            let mut used = vec![false; b.len()];
            a.iter().all(|a_elem| {
                for (i, b_elem) in b.iter().enumerate() {
                    if !used[i] && json_equal(a_elem, b_elem) {
                        used[i] = true;
                        return true;
                    }
                }
                false
            })
        }
        (serde_json::Value::Object(a), serde_json::Value::Object(b)) => {
            a.len() == b.len()
                && a.iter()
                    .all(|(k, v)| b.get(k).map(|bv| json_equal(v, bv)).unwrap_or(false))
        }
        _ => false,
    }
}
