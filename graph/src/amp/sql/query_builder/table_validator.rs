use std::collections::BTreeSet;

use anyhow::{bail, Result};
use sqlparser_latest::ast;

use super::{extract_tables, TableReference};

/// Validates that SQL query references only allowed dataset and tables.
///
/// # Errors
///
/// Returns an error if:
/// - The `query` does not reference any tables
/// - The `query` references a table not in `allowed_tables`
/// - The `query` references a dataset other than `allowed_dataset`
///
/// The returned error is deterministic.
pub(super) fn validate_tables<'a>(
    query: &ast::Query,
    allowed_dataset: &str,
    allowed_tables: impl IntoIterator<Item = &'a str>,
) -> Result<()> {
    let used_tables = extract_tables(query);

    if used_tables.is_empty() {
        bail!("query does not use any tables");
    }

    let allowed_tables = allowed_tables
        .into_iter()
        .map(|allowed_table| TableReference::raw(allowed_dataset, allowed_table))
        .collect::<BTreeSet<_>>();

    for used_table in used_tables {
        if !allowed_tables.contains(&used_table) {
            bail!("table '{used_table}' not allowed");
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::parse_query;
    use super::*;

    macro_rules! test_validate_tables {
        ($($name:ident: $input:expr, $dataset:expr, $tables:expr => $expected:expr),* $(,)?) => {
            $(
                #[test]
                fn $name() {
                    let query = parse_query($input).unwrap();
                    let result = validate_tables(&query, $dataset, $tables);

                    match $expected {
                        Result::<(), &str>::Ok(()) => {
                            result.unwrap();
                        },
                        Err(e) => {
                            assert_eq!(result.unwrap_err().to_string(), e);
                        }
                    }
                }
            )*
        };
    }

    test_validate_tables! {
        no_table_references: "SELECT *", "a", ["b"] => Err("query does not use any tables"),
        missing_dataset: "SELECT * FROM b", "a", ["b"] => Err("table 'b' not allowed"),
        missing_table: "SELECT * FROM a", "a", ["b"] => Err("table 'a' not allowed"),
        invalid_dataset: "SELECT * FROM c.b", "a", ["b"] => Err("table 'c.b' not allowed"),
        invalid_nested_dataset: "WITH a AS (SELECT * FROM c.b) SELECT * FROM a", "a", ["b"] => Err("table 'c.b' not allowed"),
        invalid_table: "SELECT * FROM a.c", "a", ["b"] => Err("table 'a.c' not allowed"),
        invalid_nested_table: "WITH a AS (SELECT * FROM a.c) SELECT * FROM a", "a", ["b"] => Err("table 'a.c' not allowed"),
        using_catalog: "SELECT * FROM c.a.b", "a", ["b"] => Err("table 'c.a.b' not allowed"),

        one_valid_table: "SELECT * FROM a.b", "a", ["b"] => Ok(()),
        one_valid_nested_table: "WITH a AS (SELECT * FROM a.b) SELECT * FROM a", "a", ["b"] => Ok(()),
        multiple_valid_tables: "SELECT * FROM a.b JOIN a.c ON a.c.c = a.b.b", "a", ["b", "c"] => Ok(()),
        multiple_valid_nested_tables: "WITH a AS (SELECT * FROM a.b JOIN a.c ON a.c.c = a.b.b) SELECT * FROM a", "a", ["b", "c"] => Ok(()),

        unquoted_dataset_is_case_insensitive: "SELECT * FROM A.b", "a", ["b"] => Ok(()),
        unquoted_tables_are_case_insensitive: "SELECT * FROM a.B", "a", ["b"] => Ok(()),

        single_quoted_dataset_is_case_sensitive: "SELECT * FROM 'A'.b", "a", ["b"] => Err(r#"table '"A".b' not allowed"#),
        single_quoted_tables_are_case_sensitive: "SELECT * FROM a.'B'", "a", ["b"] => Err(r#"table 'a."B"' not allowed"#),

        double_quoted_dataset_is_case_sensitive: r#"SELECT * FROM "A".b"#, "a", ["b"] => Err(r#"table '"A".b' not allowed"#),
        double_quoted_tables_are_case_sensitive: r#"SELECT * FROM a."B""#, "a", ["b"] => Err(r#"table 'a."B"' not allowed"#),

        backtick_quoted_dataset_is_case_sensitive: "SELECT * FROM `A`.b", "a", ["b"] => Err(r#"table '"A".b' not allowed"#),
        backtick_quoted_tables_are_case_sensitive: "SELECT * FROM a.`B`", "a", ["b"] => Err(r#"table 'a."B"' not allowed"#),

        allowed_dataset_is_case_sensitive: "SELECT * FROM a.b", "A", ["b"] => Err("table 'a.b' not allowed"),
        allowed_tables_are_case_sensitive: "SELECT * FROM a.b", "a", ["B"] => Err("table 'a.b' not allowed"),
    }
}
