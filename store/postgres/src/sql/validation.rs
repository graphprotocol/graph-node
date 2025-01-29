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

            // Change 'from table [as alias]' to 'from (select {columns} from table) as alias'
            let columns = table
                .columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            let query = if table.immutable {
                format!(
                    "select {columns} from {} where {} <= {}",
                    table.qualified_name, BLOCK_COLUMN, self.block
                )
            } else {
                format!(
                    "select {columns} from {} where {} @> {}",
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
