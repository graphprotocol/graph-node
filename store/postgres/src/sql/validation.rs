use graph::prelude::BlockNumber;
use graph::schema::AggregationInterval;
use sqlparser::ast::{
    Cte, Expr, FunctionArg, FunctionArgExpr, Ident, LimitClause, ObjectName, ObjectNamePart,
    Offset, Query, SetExpr, Statement, TableAlias, TableFactor, TableFunctionArgs, Value,
    ValueWithSpan, VisitMut, VisitorMut,
};
use sqlparser::parser::Parser;
use std::fmt::Display;
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
    #[error("Unknown aggregation interval `{1}` for table {0}")]
    UnknownAggregationInterval(String, String),
    #[error("Invalid syntax for aggregation {0}")]
    InvalidAggregationSyntax(String),
    #[error("Only constant numbers are supported for LIMIT and OFFSET.")]
    UnsupportedLimitOffset,
    #[error("The limit of {0} is greater than the maximum allowed limit of {1}.")]
    UnsupportedLimit(u32, u32),
    #[error("The offset of {0} is greater than the maximum allowed offset of {1}.")]
    UnsupportedOffset(u32, u32),
    #[error("Qualified table names are not supported: {0}")]
    NoQualifiedTables(String),
    #[error("Internal error: {0}")]
    Internal(String),
}

/// A wrapper around table names that correctly handles quoted vs unquoted
/// comparisons of names
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct TableName(String);

impl TableName {
    fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&Ident> for TableName {
    fn from(ident: &Ident) -> Self {
        let Ident {
            value,
            quote_style,
            span: _,
        } = ident;
        // Use quoted names verbatim, and normalize unquoted names to
        // lowercase
        match quote_style {
            Some(_) => Self(value.clone()),
            None => Self(value.to_lowercase()),
        }
    }
}

impl Display for TableName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Helper to track CTEs introduced by the main query or subqueries. Every
/// time we enter a query, we need to track a new set of CTEs which must be
/// discarded once we are done with that query. Otherwise, we might allow
/// access to forbidden tables with a query like `select *, (with pg_user as
/// (select 1) select 1) as one from pg_user`
#[derive(Default)]
struct CteStack {
    stack: Vec<HashSet<TableName>>,
}

impl CteStack {
    fn enter_query(&mut self) {
        self.stack.push(HashSet::new());
    }

    fn exit_query(&mut self) {
        self.stack.pop();
    }

    fn contains(&self, name: &TableName) -> bool {
        for entry in self.stack.iter().rev() {
            if entry.contains(name) {
                return true;
            }
        }
        false
    }

    fn clear(&mut self) {
        self.stack.clear();
    }

    fn add_ctes(&mut self, ctes: &[Cte]) -> ControlFlow<Error> {
        let Some(entry) = self.stack.last_mut() else {
            return ControlFlow::Break(Error::Internal("CTE stack is empty".into()));
        };
        for cte in ctes {
            entry.insert(TableName::from(&cte.alias.name));
        }
        ControlFlow::Continue(())
    }
}

pub struct Validator<'a> {
    layout: &'a Layout,
    ctes: CteStack,
    block: BlockNumber,
    max_limit: u32,
    max_offset: u32,
}

impl<'a> Validator<'a> {
    pub fn new(layout: &'a Layout, block: BlockNumber, max_limit: u32, max_offset: u32) -> Self {
        Self {
            layout,
            ctes: Default::default(),
            block,
            max_limit,
            max_offset,
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

    pub fn validate_limit_offset(&mut self, query: &mut Query) -> ControlFlow<Error> {
        let Query { limit_clause, .. } = query;

        let (limit, offset) = match limit_clause {
            None => return ControlFlow::Continue(()),
            Some(LimitClause::LimitOffset {
                limit,
                offset,
                limit_by,
            }) => {
                if !limit_by.is_empty() {
                    return ControlFlow::Break(Error::UnsupportedLimitOffset);
                }
                (limit, offset)
            }
            Some(LimitClause::OffsetCommaLimit { .. }) => {
                // MySQL syntax not supported
                return ControlFlow::Break(Error::UnsupportedLimitOffset);
            }
        };

        if let Some(limit) = limit {
            match limit {
                Expr::Value(ValueWithSpan {
                    value: Value::Number(s, _),
                    span: _,
                }) => match s.parse::<u32>() {
                    Err(_) => return ControlFlow::Break(Error::UnsupportedLimitOffset),
                    Ok(limit) => {
                        if limit > self.max_limit {
                            return ControlFlow::Break(Error::UnsupportedLimit(
                                limit,
                                self.max_limit,
                            ));
                        }
                    }
                },
                _ => return ControlFlow::Break(Error::UnsupportedLimitOffset),
            }

            if let Some(Offset { value, .. }) = offset {
                match value {
                    Expr::Value(ValueWithSpan {
                        value: Value::Number(s, _),
                        span: _,
                    }) => match s.parse::<u32>() {
                        Err(_) => return ControlFlow::Break(Error::UnsupportedLimitOffset),
                        Ok(offset) => {
                            if offset > self.max_offset {
                                return ControlFlow::Break(Error::UnsupportedOffset(
                                    offset,
                                    self.max_offset,
                                ));
                            }
                        }
                    },
                    _ => return ControlFlow::Break(Error::UnsupportedLimitOffset),
                }
            }
        }
        ControlFlow::Continue(())
    }
}

impl VisitorMut for Validator<'_> {
    type Break = Error;

    fn pre_visit_statement(&mut self, statement: &mut Statement) -> ControlFlow<Self::Break> {
        match statement {
            Statement::Query(_) => ControlFlow::Continue(()),
            _ => ControlFlow::Break(Error::NotSelectQuery),
        }
    }

    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<Self::Break> {
        // Add common table expressions to the set of known tables
        self.ctes.enter_query();
        if let Some(ref with) = query.with {
            self.ctes.add_ctes(&with.cte_tables)?;
        }

        match *query.body {
            SetExpr::Select(_) | SetExpr::Query(_) => { /* permitted */ }
            SetExpr::SetOperation { .. } => { /* permitted */ }
            SetExpr::Table(_) => { /* permitted */ }
            SetExpr::Values(_) => { /* permitted */ }
            SetExpr::Insert(_) | SetExpr::Update(_) | SetExpr::Delete(_) | SetExpr::Merge(_) => {
                return ControlFlow::Break(Error::NotSelectQuery)
            }
        }

        self.validate_limit_offset(query)
    }

    fn post_visit_query(&mut self, _query: &mut Query) -> ControlFlow<Self::Break> {
        self.ctes.exit_query();
        ControlFlow::Continue(())
    }

    /// Invoked for any table function in the AST.
    /// See [TableFactor::Table.args](sqlparser::ast::TableFactor::Table::args) for more details identifying a table function
    fn post_visit_table_factor(
        &mut self,
        table_factor: &mut TableFactor,
    ) -> ControlFlow<Self::Break> {
        /// Check whether `args` is a single string argument and return that
        /// string
        fn extract_string_arg(args: &[FunctionArg]) -> Option<String> {
            if args.len() != 1 {
                return None;
            }
            match &args[0] {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(ValueWithSpan {
                    value: Value::SingleQuotedString(s),
                    span: _,
                }))) => Some(s.clone()),
                _ => None,
            }
        }

        if let TableFactor::Table {
            name, args, alias, ..
        } = table_factor
        {
            if name.0.len() != 1 {
                // We do not support schema qualified table names
                return ControlFlow::Break(Error::NoQualifiedTables(name.to_string()));
            }
            let table_name = match &name.0[0] {
                ObjectNamePart::Identifier(ident) => TableName::from(ident),
                ObjectNamePart::Function(_) => {
                    return ControlFlow::Break(Error::NoQualifiedTables(name.to_string()));
                }
            };

            // CTES override subgraph tables
            if self.ctes.contains(&table_name) && args.is_none() {
                return ControlFlow::Continue(());
            }

            let table = match (self.layout.table(table_name.as_str()), args) {
                (None, None) => {
                    return ControlFlow::Break(Error::UnknownTable(table_name.to_string()));
                }
                (Some(_), Some(_)) => {
                    // Table exists but has args, must be a function
                    return self.validate_function_name(name);
                }
                (None, Some(args)) => {
                    // Table does not exist but has args, is either an
                    // aggregation table in the form <name>(<interval>) or
                    // must be a function

                    if !self.layout.has_aggregation(table_name.as_str()) {
                        // Not an aggregation, must be a function
                        return self.validate_function_name(name);
                    }

                    let TableFunctionArgs { args, settings } = args;
                    if settings.is_some() {
                        // We do not support settings on aggregation tables
                        return ControlFlow::Break(Error::InvalidAggregationSyntax(
                            table_name.to_string(),
                        ));
                    }
                    let Some(intv) = extract_string_arg(args) else {
                        // Looks like an aggregation, but argument is not a single string
                        return ControlFlow::Break(Error::InvalidAggregationSyntax(
                            table_name.to_string(),
                        ));
                    };
                    let Some(intv) = intv.parse::<AggregationInterval>().ok() else {
                        return ControlFlow::Break(Error::UnknownAggregationInterval(
                            table_name.to_string(),
                            intv,
                        ));
                    };

                    let Some(table) = self.layout.aggregation_table(table_name.as_str(), intv)
                    else {
                        return self.validate_function_name(name);
                    };
                    table
                }
                (Some(table), None) => {
                    if !table.object.is_object_type() {
                        // Interfaces and aggregations can not be queried
                        // with the table name directly
                        return ControlFlow::Break(Error::UnknownTable(table_name.to_string()));
                    }
                    table
                }
            };

            // Change 'from table [as alias]' to 'from (select {columns} from table) as alias'
            let columns = table
                .columns
                .iter()
                .map(|column| column.name.quoted())
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
                    explicit: true,
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
