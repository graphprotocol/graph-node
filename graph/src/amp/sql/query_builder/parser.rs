use std::ops::ControlFlow;

use anyhow::{anyhow, bail, Context, Result};
use itertools::Itertools;
use sqlparser_latest::{
    ast::{self, Visit, Visitor},
    dialect::GenericDialect,
    parser::Parser,
};

/// Parses a SQL query and returns its AST.
///
/// # Errors
///
/// Returns an error if:
/// - The SQL query cannot be parsed
/// - The SQL query contains multiple SQL statements
/// - The SQL query is not a `SELECT` query
///
/// The returned error is deterministic.
pub(super) fn parse_query(s: impl AsRef<str>) -> Result<ast::Query> {
    let statement = Parser::parse_sql(&GenericDialect {}, s.as_ref())
        .context("invalid SQL query")?
        .into_iter()
        .exactly_one()
        .map_err(|e| anyhow!("expected exactly one SQL statement, found {}", e.count()))?;

    let query = match statement {
        ast::Statement::Query(query) => *query,
        _ => bail!("invalid SQL query: only SELECT statements are allowed"),
    };

    if let ControlFlow::Break(e) = query.visit(&mut AllowOnlySelectQueries) {
        return Err(e);
    }

    Ok(query)
}

/// Validates that the SQL query AST contains only `SELECT` queries in subqueries.
struct AllowOnlySelectQueries;

impl AllowOnlySelectQueries {
    /// Returns an error if the `set_expr` is not a `SELECT` expression.
    fn visit_set_expr(&self, set_expr: &ast::SetExpr) -> Result<()> {
        match set_expr {
            ast::SetExpr::Select(_)
            | ast::SetExpr::Query(_)
            | ast::SetExpr::Values(_)
            | ast::SetExpr::Table(_) => Ok(()),
            ast::SetExpr::SetOperation { left, right, .. } => {
                self.visit_set_expr(left)?;
                self.visit_set_expr(right)?;
                Ok(())
            }
            ast::SetExpr::Insert(_) | ast::SetExpr::Update(_) | ast::SetExpr::Delete(_) => {
                bail!("invalid SQL query: only SELECT queries are allowed")
            }
        }
    }
}

impl Visitor for AllowOnlySelectQueries {
    type Break = anyhow::Error;

    fn pre_visit_query(&mut self, query: &ast::Query) -> ControlFlow<Self::Break> {
        match self.visit_set_expr(&query.body) {
            Ok(()) => ControlFlow::Continue(()),
            Err(e) => ControlFlow::Break(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! test_parse_query {
        ($($name:ident: $input:expr => $expected:expr),* $(,)?) => {
            $(
                #[test]
                fn $name() {
                    let result = parse_query($input);

                    match $expected {
                        Result::<&str, &str>::Ok(expected) => {
                            assert_eq!(result.unwrap().to_string(), expected);
                        },
                        Err(e) => {
                            assert_eq!(result.unwrap_err().to_string(), e);
                        }
                    }
                }
            )*
        };
    }

    test_parse_query! {
        invalid_query: "SELECT" => Err("invalid SQL query"),
        multiple_statements: "SELECT a FROM b; SELECT c FROM d" => Err("expected exactly one SQL statement, found 2"),
        insert_statement: "INSERT INTO a VALUES (b)" => Err("invalid SQL query: only SELECT statements are allowed"),
        update_statement: "UPDATE a SET b = c" => Err("invalid SQL query: only SELECT statements are allowed"),
        delete_statement: "DELETE FROM a WHERE b = c" => Err("invalid SQL query: only SELECT statements are allowed"),
        truncate_statement: "TRUNCATE TABLE a" => Err("invalid SQL query: only SELECT statements are allowed"),
        drop_statement: "DROP TABLE a" => Err("invalid SQL query: only SELECT statements are allowed"),

        nested_insert_query: "WITH a AS (INSERT INTO b VALUES (c) RETURNING d) SELECT * FROM a" => Err("invalid SQL query: only SELECT queries are allowed"),
        nested_update_query: "WITH a AS (UPDATE b SET c = d RETURNING e) SELECT * FROM a" => Err("invalid SQL query: only SELECT queries are allowed"),
        nested_delete_query: "WITH a AS (DELETE FROM b WHERE c = d RETURNING e) SELECT * FROM a" => Err("invalid SQL query: only SELECT queries are allowed"),

        valid_query: "SELECT a FROM b" => Ok("SELECT a FROM b"),
        valid_query_with_cte: "WITH a AS (SELECT b FROM c) SELECT * FROM a" => Ok("WITH a AS (SELECT b FROM c) SELECT * FROM a"),
        valid_query_with_join: "SELECT a FROM b INNER JOIN c ON c.c = b.b" => Ok("SELECT a FROM b INNER JOIN c ON c.c = b.b"),
    }
}
