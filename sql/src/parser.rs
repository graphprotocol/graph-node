use anyhow::{anyhow, Ok, Result};
use sqlparser::ast::Statement;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

pub struct SqlParser;
impl SqlParser {
    pub fn parse_and_validate(&self, sql: &str, _deployment_id: i32) -> Result<String> {
        let mut result = Parser::parse_sql(&PostgreSqlDialect {}, sql)?;
        let statement = result
            .get_mut(0)
            .ok_or_else(|| anyhow!("No SQL statements found"))?;

        match statement {
            Statement::Query(_query) => {
                // TODO: Validate and format the query
                let result = sql;

                Ok(format!("{}", result))
            }
            _ => Err(anyhow!("Only SELECT queries are supported")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::SqlParser;

    #[test]
    fn parse_sql() {
        let parser = SqlParser {};

        let sql = include_str!("../test.sql");

        let ast = parser.parse_and_validate(sql, 1).unwrap();

        // TODO: assert that the AST is what we expect after validation
        assert_eq!(ast, sql);
    }
}
