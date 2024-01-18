use anyhow::{Ok, Result};
use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, GroupByExpr, Query, Select, SelectItem, SetExpr,
    TableFactor, TableWithJoins,
};

const MAX_QUERY_COUNT: usize = 12;

pub struct SqlValidator;

impl SqlValidator {
    pub fn new() -> Self {
        Self {}
    }

    pub fn validate_query(&self, query: &Query) -> Result<()> {
        let mut validator = Validator::new();
        validator.validate(query)
    }
}

pub(crate) struct Validator {
    query_count: usize,
}

impl Validator {
    pub fn new() -> Self {
        Self { query_count: 0 }
    }
    fn increment(&mut self) {
        self.query_count += 1;
    }

    fn validate(&mut self, query: &Query) -> Result<()> {
        self.validate_query(query)
    }

    fn validate_query(&mut self, query: &Query) -> Result<()> {
        self.increment();
        assert!(
            self.query_count <= MAX_QUERY_COUNT,
            "Query is too complex, MAX_QUERY_COUNT : {}, exceeded, Try reducing subquery depth",
            MAX_QUERY_COUNT
        );
        match &*query.body {
            SetExpr::Select(select) => self.validate_select(&select),
            SetExpr::Query(subquery) => self.validate_query(&subquery),

            _ => unimplemented!(),
        }
    }

    fn validate_select(&mut self, select: &Select) -> Result<()> {
        for table_with_joins in &select.from {
            self.validate_table_with_joins(table_with_joins)?;
        }

        if let Some(selection) = &select.selection {
            self.validate_expr(selection)?;
        }

        for item in &select.projection {
            match item {
                SelectItem::ExprWithAlias { expr, alias: _ } => self.validate_expr(expr)?,
                SelectItem::UnnamedExpr(expr) => self.validate_expr(expr)?,
                SelectItem::Wildcard(_) => {}
                SelectItem::QualifiedWildcard(_, _) => {}
            }
        }

        for lateral_view in &select.lateral_views {
            self.validate_expr(&lateral_view.lateral_view)?
        }

        match &select.group_by {
            GroupByExpr::All => {}
            GroupByExpr::Expressions(exprs) => {
                for expr in exprs {
                    self.validate_expr(expr)?;
                }
            }
        }

        for expr in &select.cluster_by {
            self.validate_expr(expr)?;
        }

        for expr in &select.distribute_by {
            self.validate_expr(expr)?;
        }

        for expr in &select.sort_by {
            self.validate_expr(expr)?;
        }

        if let Some(having) = &select.having {
            self.validate_expr(having)?;
        }

        for _ in &select.named_window {
            unimplemented!();
        }

        if let Some(qualify) = &select.qualify {
            self.validate_expr(qualify)?;
        }

        Ok(())
    }

    fn validate_table_factor(&mut self, table_factor: &TableFactor) -> Result<()> {
        match table_factor {
            TableFactor::Table { name: _, .. } => {}
            TableFactor::Derived { subquery, .. } => {
                self.validate_query(subquery)?;
            }
            TableFactor::TableFunction { expr, .. } => {
                self.validate_expr(expr)?;
            }
            TableFactor::Function { .. } => unimplemented!(),
            TableFactor::UNNEST { array_exprs, .. } => {
                for expr in array_exprs {
                    self.validate_expr(expr)?;
                }
            }
            TableFactor::NestedJoin {
                table_with_joins, ..
            } => {
                self.validate_table_with_joins(table_with_joins)?;
            }
            TableFactor::Pivot {
                table,
                aggregate_function,
                ..
            } => {
                self.validate_table_factor(table)?;
                self.validate_expr(aggregate_function)?;
            }
            TableFactor::Unpivot { table, .. } => {
                self.validate_table_factor(table)?;
            }
        };
        Ok(())
    }

    fn validate_table_with_joins(&mut self, table_with_joins: &TableWithJoins) -> Result<()> {
        self.validate_table_factor(&table_with_joins.relation)?;

        for join in &table_with_joins.joins {
            self.validate_table_factor(&join.relation)?;
        }
        Ok(())
    }

    fn validate_expr(&mut self, expr1: &Expr) -> Result<()> {
        match expr1 {
            Expr::CompoundIdentifier(_s) => Ok(()),
            Expr::Identifier(_s) => Ok(()),
            Expr::MapAccess { column, keys } => {
                self.validate_expr(&column)?;
                for k in keys {
                    self.validate_expr(k)?;
                }

                Ok(())
            }

            Expr::IsTrue(ast)
            | Expr::IsNotTrue(ast)
            | Expr::IsFalse(ast)
            | Expr::IsNotFalse(ast)
            | Expr::IsNull(ast)
            | Expr::IsNotNull(ast)
            | Expr::IsUnknown(ast)
            | Expr::IsNotUnknown(ast) => self.validate_expr(ast),
            Expr::InList {
                expr,
                list,
                negated: _,
            } => {
                self.validate_expr(expr)?;
                for expr in list {
                    self.validate_expr(expr)?;
                }
                Ok(())
            }
            Expr::InSubquery {
                expr,
                subquery,
                negated: _,
            } => {
                self.validate_expr(expr)?;
                self.validate_query(subquery)?;
                Ok(())
            }
            Expr::InUnnest {
                expr,
                array_expr,
                negated: _,
            } => {
                self.validate_expr(expr)?;
                self.validate_expr(array_expr)?;
                Ok(())
            }
            Expr::Between {
                expr,
                negated: _,
                low,
                high,
            } => {
                self.validate_expr(expr)?;
                self.validate_expr(low)?;
                self.validate_expr(high)?;
                Ok(())
            }
            Expr::BinaryOp { left, op: _, right } => {
                self.validate_expr(left)?;
                self.validate_expr(right)?;
                Ok(())
            }
            Expr::Like {
                negated: _,
                expr,
                pattern,
                escape_char: _,
            } => {
                self.validate_expr(expr)?;
                self.validate_expr(pattern)?;
                Ok(())
            }
            Expr::ILike {
                negated: _,
                expr,
                pattern,
                escape_char: _,
            } => {
                self.validate_expr(expr)?;
                self.validate_expr(pattern)?;
                Ok(())
            }
            Expr::RLike {
                negated: _,
                expr,
                pattern,
                regexp: _,
            } => {
                self.validate_expr(expr)?;
                self.validate_expr(pattern)?;

                Ok(())
            }
            Expr::SimilarTo {
                negated: _,
                expr,
                pattern,
                escape_char: _,
            } => {
                self.validate_expr(expr)?;
                self.validate_expr(pattern)?;
                Ok(())
            }
            Expr::AnyOp {
                left,
                compare_op: _,
                right,
            } => {
                self.validate_expr(left)?;
                self.validate_expr(right)?;
                Ok(())
            }
            Expr::AllOp {
                left,
                compare_op: _,
                right,
            } => {
                self.validate_expr(left)?;
                self.validate_expr(right)?;
                Ok(())
            }
            Expr::UnaryOp { op: _, expr } => {
                self.validate_expr(expr)?;
                Ok(())
            }
            Expr::Convert {
                expr,
                target_before_value: _,
                data_type: _,
                charset: _,
            } => {
                self.validate_expr(expr)?;
                Ok(())
            }

            Expr::Cast {
                expr,
                data_type: _,
                format: _,
            } => {
                self.validate_expr(expr)?;
                Ok(())
            }

            Expr::TryCast {
                expr,
                data_type: _,
                format: _,
            } => {
                self.validate_expr(expr)?;
                Ok(())
            }

            Expr::SafeCast {
                expr,
                data_type: _,
                format: _,
            } => {
                self.validate_expr(expr)?;
                Ok(())
            }

            Expr::Extract { field: _, expr } => {
                self.validate_expr(expr)?;
                Ok(())
            }

            Expr::Ceil { expr, field: _ } => {
                self.validate_expr(expr)?;
                Ok(())
            }

            Expr::Floor { expr, field: _ } => {
                self.validate_expr(expr)?;
                Ok(())
            }
            Expr::Position { expr, r#in } => {
                self.validate_expr(expr)?;
                self.validate_expr(r#in)?;
                Ok(())
            }

            Expr::Collate { expr, collation: _ } => {
                self.validate_expr(expr)?;
                Ok(())
            }

            Expr::Nested(ast) => {
                self.validate_expr(ast)?;
                Ok(())
            }

            Expr::Value(_v) => Ok(()),

            Expr::IntroducedString {
                introducer: _,
                value: _,
            } => Ok(()),

            Expr::TypedString {
                data_type: _,
                value: _,
            } => Ok(()),

            Expr::Function(fun) => {
                let arg = &fun.args;

                for arg in arg.iter() {
                    match arg {
                        FunctionArg::Unnamed(arg) => match arg {
                            FunctionArgExpr::Expr(expr) => {
                                self.validate_expr(expr)?;
                            }
                            FunctionArgExpr::QualifiedWildcard(_) => {}

                            FunctionArgExpr::Wildcard => {}
                        },
                        FunctionArg::Named { arg, .. } => match arg {
                            FunctionArgExpr::Expr(expr) => {
                                self.validate_expr(expr)?;
                            }
                            FunctionArgExpr::QualifiedWildcard(_) => {}
                            FunctionArgExpr::Wildcard => {}
                        },
                    }
                }

                if let Some(filter) = &fun.filter {
                    self.validate_expr(filter)?;
                }

                Ok(())
            }

            Expr::AggregateExpressionWithFilter { expr, filter } => {
                self.validate_expr(expr)?;
                self.validate_expr(filter)?;
                Ok(())
            }
            Expr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => {
                if let Some(operand) = operand {
                    self.validate_expr(operand)?;
                }
                for (c, r) in conditions.iter().zip(results) {
                    self.validate_expr(c)?;
                    self.validate_expr(r)?;
                }
                if let Some(else_result) = else_result {
                    self.validate_expr(else_result)?;
                }
                Ok(())
            }

            Expr::Exists {
                subquery,
                negated: _,
            } => {
                self.validate_query(subquery)?;
                Ok(())
            }

            Expr::Subquery(s) => {
                self.validate_query(s)?;
                Ok(())
            }

            Expr::ArraySubquery(s) => {
                self.validate_query(s)?;
                Ok(())
            }

            Expr::ListAgg(_listagg) => unimplemented!(),

            Expr::ArrayAgg(_arrayagg) => unimplemented!(),

            Expr::GroupingSets(_sets) => unimplemented!(),

            Expr::Cube(_sets) => unimplemented!(),

            Expr::Rollup(_sets) => unimplemented!(),

            Expr::Substring {
                expr,
                substring_from,
                substring_for,
                special: _,
            } => {
                self.validate_expr(expr)?;
                if let Some(from_part) = substring_from {
                    self.validate_expr(from_part)?;
                }
                if let Some(for_part) = substring_for {
                    self.validate_expr(for_part)?;
                }
                Ok(())
            }

            Expr::Overlay {
                expr,
                overlay_what,
                overlay_from,
                overlay_for,
            } => {
                self.validate_expr(expr)?;
                self.validate_expr(overlay_what)?;
                self.validate_expr(overlay_from)?;
                if let Some(for_part) = overlay_for {
                    self.validate_expr(for_part)?;
                }
                Ok(())
            }

            Expr::IsDistinctFrom(a, b) | Expr::IsNotDistinctFrom(a, b) => {
                self.validate_expr(a)?;
                self.validate_expr(b)?;
                Ok(())
            }

            Expr::Trim {
                expr,
                trim_where: _,
                trim_what,
                trim_characters: _,
            } => {
                self.validate_expr(expr)?;
                if let Some(trim_char) = trim_what {
                    self.validate_expr(trim_char)?;
                }
                Ok(())
            }

            Expr::Tuple(exprs) => {
                for expr in exprs {
                    self.validate_expr(expr)?;
                }
                Ok(())
            }
            Expr::Struct { values, fields: _ } => {
                for value in values {
                    self.validate_expr(value)?;
                }
                Ok(())
            }

            Expr::Named { expr, name: _ } => {
                self.validate_expr(expr)?;
                Ok(())
            }

            Expr::ArrayIndex { obj, indexes } => {
                self.validate_expr(obj)?;
                for i in indexes {
                    self.validate_expr(i)?;
                }
                Ok(())
            }

            Expr::Array(set) => {
                for expr in set.elem.iter() {
                    self.validate_expr(expr)?;
                }
                Ok(())
            }

            Expr::JsonAccess {
                left,
                operator: _,
                right,
            } => {
                self.validate_expr(left)?;
                self.validate_expr(right)?;
                Ok(())
            }

            Expr::CompositeAccess { expr, key: _ } => {
                self.validate_expr(expr)?;
                Ok(())
            }

            Expr::AtTimeZone {
                timestamp,
                time_zone: _,
            } => {
                self.validate_expr(timestamp)?;
                Ok(())
            }

            Expr::Interval(interval) => self.validate_expr(&interval.value),
            Expr::MatchAgainst {
                columns: _,
                match_value: _,
                opt_search_modifier: _,
            } => Ok(()),
        }
    }
}
