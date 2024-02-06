use sqlparser::ast::{Expr, ObjectName, Query, SetExpr, Statement, TableFactor, Visit, Visitor};
use std::result::Result;
use std::{collections::HashSet, ops::ControlFlow};

use super::{
    constants::{POSTGRES_BLACKLISTED_FUNCTIONS, POSTGRES_WHITELISTED_FUNCTIONS},
    Schema,
};

#[derive(thiserror::Error, Debug, PartialEq)]
pub enum Error {
    #[error("Unknown function {0}")]
    UnknownFunction(String),
    #[error("Blacklisted function {0}")]
    BlackListedFunction(String),
    #[error("Multi statement is not supported.")]
    MultiStatementUnSupported,
    #[error("Only SELECT query is supported.")]
    NotSelectQuery,
    #[error("Unknown table {0}")]
    UnknownTable(String),
}

pub struct Validator<'a> {
    schema: &'a Schema,
    ctes: HashSet<String>,
}

impl<'a> Validator<'a> {
    pub fn new(schema: &'a Schema) -> Self {
        Self {
            schema,
            ctes: Default::default(),
        }
    }

    fn validate_function_name(&self, name: &ObjectName) -> ControlFlow<Error> {
        let name = name.to_string().to_lowercase();
        if POSTGRES_WHITELISTED_FUNCTIONS.contains(name.as_str()) {
            ControlFlow::Continue(())
        } else if POSTGRES_BLACKLISTED_FUNCTIONS.contains(name.as_str()) {
            ControlFlow::Break(Error::BlackListedFunction(name))
        } else {
            ControlFlow::Break(Error::UnknownFunction(name))
        }
    }

    pub fn validate_statements(&mut self, statements: &Vec<Statement>) -> Result<(), Error> {
        self.ctes.clear();

        if statements.len() > 1 {
            return Err(Error::MultiStatementUnSupported);
        }

        if let ControlFlow::Break(error) = statements.visit(self) {
            return Err(error);
        }

        Ok(())
    }

    fn validate_table_name(&mut self, name: &ObjectName) -> ControlFlow<Error> {
        if let Some(table_name) = name.0.last() {
            let table_name = table_name.to_string().to_lowercase();
            if !self.schema.contains_key(&table_name) && !self.ctes.contains(&table_name) {
                return ControlFlow::Break(Error::UnknownTable(table_name));
            }
        }
        ControlFlow::Continue(())
    }
}

impl Visitor for Validator<'_> {
    type Break = Error;

    fn pre_visit_statement(&mut self, _statement: &Statement) -> ControlFlow<Self::Break> {
        match _statement {
            Statement::Query(_) => ControlFlow::Continue(()),
            _ => ControlFlow::Break(Error::NotSelectQuery),
        }
    }

    fn pre_visit_query(&mut self, _query: &Query) -> ControlFlow<Self::Break> {
        // Add common table expressions to the set of known tables
        if let Some(ref with) = _query.with {
            self.ctes.extend(
                with.cte_tables
                    .iter()
                    .map(|cte| cte.alias.name.value.to_lowercase()),
            );
        }

        match *_query.body {
            SetExpr::Update(_) | SetExpr::Insert(_) => ControlFlow::Break(Error::NotSelectQuery),
            _ => ControlFlow::Continue(()),
        }
    }

    /// Invoked for any table function in the AST.
    /// See [TableFactor::Table.args](sqlparser::ast::TableFactor::Table::args) for more details identifying a table function
    fn pre_visit_table_factor(&mut self, _table_factor: &TableFactor) -> ControlFlow<Self::Break> {
        if let TableFactor::Table { name, args, .. } = _table_factor {
            if args.is_some() {
                return self.validate_function_name(name);
            } else {
                return self.validate_table_name(name);
            }
        }
        ControlFlow::Continue(())
    }

    /// Invoked for any function expressions that appear in the AST
    fn pre_visit_expr(&mut self, _expr: &Expr) -> ControlFlow<Self::Break> {
        if let Expr::Function(function) = _expr {
            return self.validate_function_name(&function.name);
        }
        ControlFlow::Continue(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::sql::constants::SQL_DIALECT;
    use std::collections::{HashMap, HashSet};

    fn validate(sql: &str) -> Result<(), Error> {
        let statements = sqlparser::parser::Parser::parse_sql(&SQL_DIALECT, sql).unwrap();

        let schema: Schema = HashMap::from([(
            "swap".to_owned(),
            HashSet::from([
                "vid".to_owned(),
                "block$".to_owned(),
                "id".to_owned(),
                "sender".to_owned(),
                "input_amount".to_owned(),
                "input_token".to_owned(),
                "amount_out".to_owned(),
                "output_token".to_owned(),
                "slippage".to_owned(),
                "referral_code".to_owned(),
                "block_number".to_owned(),
                "block_timestamp".to_owned(),
                "transaction_hash".to_owned(),
            ]),
        )]);

        let mut validator = Validator::new(&schema);

        validator.validate_statements(&statements)
    }

    #[test]
    fn test_function_blacklisted() {
        let result = validate(
            "
            SELECT 
                input_token 
            FROM swap 
            WHERE '' = (
                SELECT 
                    CAST(pg_sleep(5) AS text
                )
            )",
        );
        assert_eq!(
            result,
            Err(Error::BlackListedFunction("pg_sleep".to_owned()))
        );
    }

    #[test]
    fn test_table_function_blacklisted() {
        let result = validate(
            "
        SELECT 
            vid,
            k.sname 
        FROM swap,
        LATERAL(
            SELECT 
                current_schemas as sname 
            FROM current_schemas(true)
        ) as k",
        );
        assert_eq!(
            result,
            Err(Error::BlackListedFunction("current_schemas".to_owned()))
        );
    }

    #[test]
    fn test_function_blacklisted_without_paranthesis() {
        let result = validate(
            "
            SELECT 
                input_token 
            FROM swap 
            WHERE '' = (
                SELECT user
            )",
        );
        assert_eq!(result, Err(Error::BlackListedFunction("user".to_owned())));
    }

    #[test]
    fn test_function_whitelisted() {
        let result = validate(
            "
            SELECT 
                input_token,
                SUM(input_amount) AS total_amount
            FROM swap 
            GROUP BY input_token
            HAVING SUM(input_amount) > 1000
            ",
        );
        assert_eq!(result, Ok(()));
    }

    #[test]
    fn test_function_unknown() {
        let result = validate(
            "
            SELECT 
                input_token 
            FROM swap 
            WHERE '' = (
                SELECT 
                    CAST(do_strange_math(amount_in) AS text
                )
            )",
        );
        assert_eq!(
            result,
            Err(Error::UnknownFunction("do_strange_math".to_owned()))
        );
    }

    #[test]
    fn test_not_select_ddl() {
        let result = validate(
            "
            CREATE TABLE foo (id INT PRIMARY KEY);
            ",
        );
        assert_eq!(result, Err(Error::NotSelectQuery));
    }

    #[test]
    fn test_not_select_insert() {
        let result = validate(
            "
            INSERT INTO foo VALUES (1);
            ",
        );
        assert_eq!(result, Err(Error::NotSelectQuery));
    }

    #[test]
    fn test_common_table_expression() {
        let result = validate(
            "
            WITH foo AS (SELECT 1) SELECT * FROM foo;
            ",
        );
        assert_eq!(result, Ok(()));
    }

    #[test]
    fn test_no_multi_statement() {
        let result = validate(
            "
            SELECT 1; SELECT 2;
            ",
        );
        assert_eq!(result, Err(Error::MultiStatementUnSupported));
    }

    #[test]
    fn test_table_unknown() {
        let result = validate(
            "
            SELECT * FROM unknown_table;
            ",
        );
        assert_eq!(result, Err(Error::UnknownTable("unknown_table".to_owned())));
    }
}
