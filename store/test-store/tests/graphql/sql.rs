// SQL Query Tests for Graph Node
// These tests parallel the GraphQL tests in query.rs but use SQL queries

use graph::components::store::QueryStoreManager;
use graph::data::query::QueryTarget;
use graph::data::store::SqlQueryObject;
use graph::prelude::{r, QueryExecutionError};
use std::collections::BTreeSet;
use test_store::{run_test_sequentially, STORE};

#[cfg(debug_assertions)]
use graph::env::ENV_VARS;

// Import test setup from query.rs module
use super::query::{setup, IdType};

/// Synchronous wrapper for SQL query execution
fn run_sql_query<F>(sql: &str, test: F)
where
    F: Fn(Result<Vec<SqlQueryObject>, QueryExecutionError>, IdType) + Send + 'static,
{
    let sql = sql.to_string(); // Convert to owned String
    run_test_sequentially(move |store| async move {
        ENV_VARS.enable_sql_queries_for_tests(true);

        for id_type in [IdType::String, IdType::Bytes, IdType::Int8] {
            let name = id_type.deployment_id();
            let deployment = setup(store.as_ref(), name, BTreeSet::new(), id_type).await;

            let query_store = STORE
                .query_store(QueryTarget::Deployment(
                    deployment.hash.clone(),
                    Default::default(),
                ))
                .await
                .unwrap();

            let result = query_store.execute_sql(&sql);
            test(result, id_type);
        }

        ENV_VARS.enable_sql_queries_for_tests(false);
    });
}

#[test]
fn sql_can_query_simple_select() {
    const SQL: &str = "SELECT id, name FROM musician ORDER BY id";

    run_sql_query(SQL, |result, _| {
        let results = result.expect("SQL query should succeed");
        assert_eq!(results.len(), 5, "Should return 5 musicians");

        // Check first musician
        if let Some(first) = results.first() {
            if let r::Value::Object(ref obj) = first.0 {
                if let Some(r::Value::String(name)) = obj.get("name") {
                    assert_eq!(name, "John", "First musician should be John");
                }
            }
        }
    });
}

#[test]
fn sql_can_query_with_where_clause() {
    const SQL: &str = "SELECT id, name FROM musician WHERE name = 'John'";

    run_sql_query(SQL, |result, _| {
        let results = result.expect("SQL query should succeed");
        assert_eq!(results.len(), 1, "Should return 1 musician named John");

        if let Some(first) = results.first() {
            if let r::Value::Object(ref obj) = first.0 {
                if let Some(r::Value::String(name)) = obj.get("name") {
                    assert_eq!(name, "John", "Should return John");
                }
            }
        }
    });
}

#[test]
fn sql_can_query_with_aggregation() {
    const SQL: &str = "SELECT COUNT(*) as total FROM musician";

    run_sql_query(SQL, |result, _| {
        let results = result.expect("SQL query should succeed");
        assert_eq!(results.len(), 1, "Should return 1 row with count");

        if let Some(first) = results.first() {
            if let r::Value::Object(ref obj) = first.0 {
                if let Some(total) = obj.get("total") {
                    // The count should be a number (could be various forms)
                    match total {
                        r::Value::Int(n) => assert_eq!(*n, 5),
                        r::Value::String(s) => assert_eq!(s, "5"),
                        _ => panic!("Total should be a number: {:?}", total),
                    }
                }
            }
        }
    });
}

#[test]
fn sql_can_query_with_limit_offset() {
    const SQL: &str = "SELECT id, name FROM musician ORDER BY id LIMIT 2 OFFSET 1";

    run_sql_query(SQL, |result, _| {
        let results = result.expect("SQL query should succeed");
        assert_eq!(results.len(), 2, "Should return 2 musicians with offset");

        // Should skip first musician (order may vary by id type)
        if let Some(first) = results.first() {
            if let r::Value::Object(ref obj) = first.0 {
                if let Some(r::Value::String(name)) = obj.get("name") {
                    // Just check we got a valid musician name
                    assert!(["John", "Lisa", "Tom", "Valerie", "Paul"].contains(&name.as_str()));
                }
            }
        }
    });
}

#[test]
fn sql_can_query_with_group_by() {
    const SQL: &str = "
        SELECT COUNT(*) as musician_count
        FROM musician
        GROUP BY name
        ORDER BY musician_count DESC
    ";

    run_sql_query(SQL, |result, _| {
        let results = result.expect("SQL query should succeed");
        assert!(!results.is_empty(), "Should return grouped musician counts");
    });
}

// Validation Tests

#[test]
fn sql_validates_table_names() {
    const SQL: &str = "SELECT * FROM invalid_table";

    run_sql_query(SQL, |result, _| {
        assert!(result.is_err(), "Query with invalid table should fail");
        if let Err(e) = result {
            let error_msg = e.to_string();
            assert!(
                error_msg.contains("Unknown table") || error_msg.contains("invalid_table"),
                "Error should mention unknown table: {}",
                error_msg
            );
        }
    });
}

#[test]
fn sql_validates_functions() {
    // Try to use a potentially dangerous function
    const SQL: &str = "SELECT pg_sleep(1)";

    run_sql_query(SQL, |result, _| {
        assert!(result.is_err(), "Query with blocked function should fail");
        if let Err(e) = result {
            let error_msg = e.to_string();
            assert!(
                error_msg.contains("Unknown or unsupported function")
                    || error_msg.contains("pg_sleep"),
                "Error should mention unsupported function: {}",
                error_msg
            );
        }
    });
}

#[test]
fn sql_blocks_ddl_statements() {
    const SQL: &str = "DROP TABLE musician";

    run_sql_query(SQL, |result, _| {
        assert!(result.is_err(), "DDL statements should be blocked");
        if let Err(e) = result {
            let error_msg = e.to_string();
            assert!(
                error_msg.contains("Only SELECT query is supported") || error_msg.contains("DROP"),
                "Error should mention unsupported statement type: {}",
                error_msg
            );
        }
    });
}

#[test]
fn sql_blocks_dml_statements() {
    const SQL: &str = "DELETE FROM musician WHERE id = 'm1'";

    run_sql_query(SQL, |result, _| {
        assert!(result.is_err(), "DML statements should be blocked");
        if let Err(e) = result {
            let error_msg = e.to_string();
            assert!(
                error_msg.contains("Only SELECT query is supported")
                    || error_msg.contains("DELETE"),
                "Error should mention unsupported statement type: {}",
                error_msg
            );
        }
    });
}

#[test]
fn sql_blocks_multi_statement() {
    const SQL: &str = "SELECT * FROM musician; SELECT * FROM band";

    run_sql_query(SQL, |result, _| {
        assert!(result.is_err(), "Multi-statement queries should be blocked");
        if let Err(e) = result {
            let error_msg = e.to_string();
            assert!(
                error_msg.contains("Multi statement is not supported")
                    || error_msg.contains("multiple statements"),
                "Error should mention multi-statement restriction: {}",
                error_msg
            );
        }
    });
}

#[test]
fn sql_can_query_with_case_expression() {
    const SQL: &str = "
        SELECT
            id,
            name,
            CASE
                WHEN favorite_count > 10 THEN 'popular'
                WHEN favorite_count > 5 THEN 'liked'
                ELSE 'normal'
            END as popularity
        FROM musician
        ORDER BY id
        LIMIT 5
    ";

    run_sql_query(SQL, |result, _| {
        let results = result.expect("SQL query with CASE should succeed");
        assert!(
            results.len() <= 5,
            "Should return limited musicians with popularity"
        );

        // Check that popularity field exists in first result
        if let Some(first) = results.first() {
            if let r::Value::Object(ref obj) = first.0 {
                assert!(
                    obj.get("popularity").is_some(),
                    "Should have popularity field"
                );
            }
        }
    });
}

#[test]
fn sql_can_query_with_subquery() {
    const SQL: &str = "
        WITH active_musicians AS (
            SELECT id, name
            FROM musician
            WHERE name IS NOT NULL
        )
        SELECT COUNT(*) as active_count FROM active_musicians
    ";

    run_sql_query(SQL, |result, _| {
        let results = result.expect("SQL query with CTE should succeed");
        assert_eq!(results.len(), 1, "Should return one count result");

        if let Some(first) = results.first() {
            if let r::Value::Object(ref obj) = first.0 {
                let count = obj.get("active_count");
                assert!(count.is_some(), "Should have active_count field");
            }
        }
    });
}
