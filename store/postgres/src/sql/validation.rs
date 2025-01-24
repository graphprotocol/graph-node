use graph::prelude::BlockNumber;
use sqlparser::ast::{
    Expr, Ident, ObjectName, Query, SetExpr, Statement, TableAlias, TableFactor, VisitMut,
    VisitorMut,
};
use sqlparser::parser::Parser;
use std::result::Result;
use std::{collections::HashSet, ops::ControlFlow};

use crate::block_range::{BLOCK_COLUMN, BLOCK_RANGE_COLUMN};
use crate::relational::Layout;

use super::constants::{ALLOWED_FUNCTIONS, SQL_DIALECT};

#[derive(thiserror::Error, Debug, PartialEq)]
pub enum Error {
    #[error("Unknown or unsupported function {0}")]
    UnknownFunction(String),
    #[error("Multi statement is not supported.")]
    MultiStatementUnSupported,
    #[error("Only SELECT query is supported.")]
    NotSelectQuery,
    #[error("Unknown table {0}")]
    UnknownTable(String),
}

pub struct Validator<'a> {
    layout: &'a Layout,
    ctes: HashSet<String>,
    block: BlockNumber,
}

impl<'a> Validator<'a> {
    pub fn new(layout: &'a Layout, block: BlockNumber) -> Self {
        Self {
            layout,
            ctes: Default::default(),
            block,
        }
    }

    fn validate_function_name(&self, name: &ObjectName) -> ControlFlow<Error> {
        let name = name.to_string().to_lowercase();
        if ALLOWED_FUNCTIONS.contains(name.as_str()) {
            ControlFlow::Continue(())
        } else {
            ControlFlow::Break(Error::UnknownFunction(name))
        }
    }

    pub fn validate_statements(&mut self, statements: &mut Vec<Statement>) -> Result<(), Error> {
        self.ctes.clear();

        if statements.len() > 1 {
            return Err(Error::MultiStatementUnSupported);
        }

        if let ControlFlow::Break(error) = statements.visit(self) {
            return Err(error);
        }

        Ok(())
    }
}

impl VisitorMut for Validator<'_> {
    type Break = Error;

    fn pre_visit_statement(&mut self, _statement: &mut Statement) -> ControlFlow<Self::Break> {
        match _statement {
            Statement::Query(_) => ControlFlow::Continue(()),
            _ => ControlFlow::Break(Error::NotSelectQuery),
        }
    }

    fn pre_visit_query(&mut self, _query: &mut Query) -> ControlFlow<Self::Break> {
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
    fn post_visit_table_factor(
        &mut self,
        table_factor: &mut TableFactor,
    ) -> ControlFlow<Self::Break> {
        if let TableFactor::Table {
            name, args, alias, ..
        } = table_factor
        {
            if args.is_some() {
                return self.validate_function_name(name);
            }
            let table = if let Some(table_name) = name.0.last() {
                let name = &table_name.value;
                let Some(table) = self.layout.table(name) else {
                    if self.ctes.contains(name) {
                        return ControlFlow::Continue(());
                    } else {
                        return ControlFlow::Break(Error::UnknownTable(name.to_string()));
                    }
                };
                table
            } else {
                return ControlFlow::Continue(());
            };

            // Change 'from table [as alias]' to 'from (select * from table) as alias'
            let query = if table.immutable {
                format!(
                    "select * from {} where {} <= {}",
                    table.qualified_name, BLOCK_COLUMN, self.block
                )
            } else {
                format!(
                    "select * from {} where {} @> {}",
                    table.qualified_name, BLOCK_RANGE_COLUMN, self.block
                )
            };
            let Statement::Query(subquery) = Parser::parse_sql(&SQL_DIALECT, &query)
                .unwrap()
                .pop()
                .unwrap()
            else {
                unreachable!();
            };
            let alias = alias.as_ref().map(|alias| alias.clone()).or_else(|| {
                Some(TableAlias {
                    name: Ident::new(table.name.as_str()),
                    columns: vec![],
                })
            });
            *table_factor = TableFactor::Derived {
                lateral: false,
                subquery,
                alias,
            };
        }
        ControlFlow::Continue(())
    }

    /// Invoked for any function expressions that appear in the AST
    fn pre_visit_expr(&mut self, _expr: &mut Expr) -> ControlFlow<Self::Break> {
        if let Expr::Function(function) = _expr {
            return self.validate_function_name(&function.name);
        }
        ControlFlow::Continue(())
    }
}

#[cfg(test)]
mod test {
    use graph::prelude::BLOCK_NUMBER_MAX;

    use super::*;
    use crate::sql::{constants::SQL_DIALECT, test::make_layout};

    fn validate(sql: &str) -> Result<(), Error> {
        let mut statements = sqlparser::parser::Parser::parse_sql(&SQL_DIALECT, sql).unwrap();

        const GQL: &str = "
            type Swap @entity {
                id: ID!
                sender: Bytes!
                inputAmount: BigDecimal!
                inputToken: Bytes!
                amountOut: BigDecimal!
                outputToken: Bytes!
                slippage: BigDecimal!
                referralCode: String
                blockNumber: Int!
                blockTimestamp: Timestamp!
                transactionHash: Bytes!
            }";

        let layout = make_layout(GQL);

        let mut validator = Validator::new(&layout, BLOCK_NUMBER_MAX);

        validator.validate_statements(&mut statements)
    }

    #[test]
    fn test_function_disallowed() {
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
        assert_eq!(result, Err(Error::UnknownFunction("pg_sleep".to_owned())));
    }

    #[test]
    fn test_table_function_disallowed() {
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
            Err(Error::UnknownFunction("current_schemas".to_owned()))
        );
    }

    #[test]
    fn test_function_disallowed_without_paranthesis() {
        let result = validate(
            "
            SELECT
                input_token
            FROM swap
            WHERE '' = (
                SELECT user
            )",
        );
        assert_eq!(result, Err(Error::UnknownFunction("user".to_owned())));
    }

    #[test]
    fn test_function_allowed() {
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
    fn test_common_table_expression_with_effect() {
        let result = validate(
            "
            WITH foo AS (INSERT INTO target VALUES(1)) SELECT * FROM bar;
            ",
        );
        assert_eq!(result, Err(Error::NotSelectQuery));
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
