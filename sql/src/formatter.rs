use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, GroupByExpr, ObjectName, Query, Select, SelectItem,
    SetExpr, TableFactor, TableWithJoins,
};

pub struct SqlFormatter;

impl SqlFormatter {
    pub fn format(&self, query: &mut Query, prefix: Option<&str>) -> String {
        self.format_query(query, prefix);

        format!("SELECT to_jsonb(sub.*) AS data FROM ( {} ) AS sub;", query)
    }

    fn format_query(&self, query: &mut Query, prefix: Option<&str>) {
        match &mut *query.body {
            SetExpr::Select(select) => {
                self.format_select(&mut *select, prefix);
            }
            SetExpr::Query(subquery) => {
                self.format_query(subquery, prefix);
            }
            _ => unimplemented!(),
        }
    }

    fn format_select(&self, select: &mut Select, prefix: Option<&str>) {
        for table_with_joins in &mut select.from {
            self.format_table_with_joins(table_with_joins, prefix);
        }

        if let Some(selection) = &mut select.selection {
            self.format_expr(selection, prefix);
        }

        for item in &mut select.projection {
            match item {
                SelectItem::ExprWithAlias { expr, alias: _ } => self.format_expr(expr, prefix),
                SelectItem::UnnamedExpr(expr) => self.format_expr(expr, prefix),
                SelectItem::Wildcard(_) => {}
                SelectItem::QualifiedWildcard(_, _) => {}
            }
        }

        for lateral_view in &mut select.lateral_views {
            self.format_expr(&mut lateral_view.lateral_view, prefix)
        }

        match &mut select.group_by {
            GroupByExpr::All => {}
            GroupByExpr::Expressions(exprs) => {
                for expr in exprs {
                    self.format_expr(expr, prefix);
                }
            }
        }

        for expr in &mut select.cluster_by {
            self.format_expr(expr, prefix);
        }

        for expr in &mut select.distribute_by {
            self.format_expr(expr, prefix);
        }

        for expr in &mut select.sort_by {
            self.format_expr(expr, prefix);
        }

        if let Some(having) = &mut select.having {
            self.format_expr(having, prefix);
        }

        for _ in &mut select.named_window {
            unimplemented!();
        }

        if let Some(qualify) = &mut select.qualify {
            self.format_expr(qualify, prefix);
        }
    }

    fn format_table_factor(&self, table_factor: &mut TableFactor, prefix: Option<&str>) {
        match table_factor {
            TableFactor::Table { name, .. } => {
                if let Some(prefix) = prefix {
                    *name = self.prepend_prefix_to_object_name(name, prefix);
                }
            }
            TableFactor::Derived { subquery, .. } => {
                self.format_query(subquery, prefix);
            }
            TableFactor::TableFunction { expr, .. } => {
                self.format_expr(expr, prefix);
            }
            TableFactor::Function { .. } => unimplemented!(),
            TableFactor::UNNEST { array_exprs, .. } => {
                for expr in array_exprs {
                    self.format_expr(expr, prefix);
                }
            }
            TableFactor::NestedJoin {
                table_with_joins, ..
            } => {
                self.format_table_with_joins(table_with_joins, prefix);
            }
            TableFactor::Pivot {
                table,
                aggregate_function,
                ..
            } => {
                self.format_table_factor(table, prefix);
                self.format_expr(aggregate_function, prefix);
            }
            TableFactor::Unpivot { table, .. } => {
                self.format_table_factor(table, prefix);
            }
        }
    }

    fn format_table_with_joins(&self, table_with_joins: &mut TableWithJoins, prefix: Option<&str>) {
        self.format_table_factor(&mut table_with_joins.relation, prefix);

        for join in &mut table_with_joins.joins {
            self.format_table_factor(&mut join.relation, prefix);
        }
    }

    fn prepend_prefix_to_object_name(&self, name: &ObjectName, prefix: &str) -> ObjectName {
        let mut new_name = name.0.clone();
        if let Some(ident) = new_name.last_mut() {
            ident.value = format!("{}.{}", prefix, ident.value);
        }
        ObjectName(new_name)
    }

    fn format_expr(&self, expr1: &mut Expr, prefix: Option<&str>) {
        match expr1 {
            Expr::CompoundIdentifier(_s) => {}
            Expr::Identifier(_s) => {}
            Expr::MapAccess { column, keys } => {
                self.format_expr(column, prefix);
                for k in keys {
                    self.format_expr(k, prefix);
                }
            }

            Expr::IsTrue(ast)
            | Expr::IsNotTrue(ast)
            | Expr::IsFalse(ast)
            | Expr::IsNotFalse(ast)
            | Expr::IsNull(ast)
            | Expr::IsNotNull(ast)
            | Expr::IsUnknown(ast)
            | Expr::IsNotUnknown(ast) => {
                self.format_expr(ast, prefix);
            }
            Expr::InList {
                expr,
                list,
                negated: _,
            } => {
                self.format_expr(expr, prefix);
                for expr in list {
                    self.format_expr(expr, prefix);
                }
            }
            Expr::InSubquery {
                expr,
                subquery,
                negated: _,
            } => {
                self.format_expr(expr, prefix);
                self.format_query(subquery, prefix);
            }
            Expr::InUnnest {
                expr,
                array_expr,
                negated: _,
            } => {
                self.format_expr(expr, prefix);
                self.format_expr(array_expr, prefix);
            }
            Expr::Between {
                expr,
                negated: _,
                low,
                high,
            } => {
                self.format_expr(expr, prefix);
                self.format_expr(low, prefix);
                self.format_expr(high, prefix);
            }
            Expr::BinaryOp { left, op: _, right } => {
                self.format_expr(left, prefix);
                self.format_expr(right, prefix);
            }
            Expr::Like {
                negated: _,
                expr,
                pattern,
                escape_char: _,
            } => {
                self.format_expr(expr, prefix);
                self.format_expr(pattern, prefix);
            }
            Expr::ILike {
                negated: _,
                expr,
                pattern,
                escape_char: _,
            } => {
                self.format_expr(expr, prefix);
                self.format_expr(pattern, prefix);
            }
            Expr::RLike {
                negated: _,
                expr,
                pattern,
                regexp: _,
            } => {
                self.format_expr(expr, prefix);
                self.format_expr(pattern, prefix);
            }
            Expr::SimilarTo {
                negated: _,
                expr,
                pattern,
                escape_char: _,
            } => {
                self.format_expr(expr, prefix);
                self.format_expr(pattern, prefix);
            }
            Expr::AnyOp {
                left,
                compare_op: _,
                right,
            } => {
                self.format_expr(left, prefix);
                self.format_expr(right, prefix);
            }
            Expr::AllOp {
                left,
                compare_op: _,
                right,
            } => {
                self.format_expr(left, prefix);
                self.format_expr(right, prefix);
            }
            Expr::UnaryOp { op: _, expr } => {
                self.format_expr(expr, prefix);
            }
            Expr::Convert {
                expr,
                target_before_value: _,
                data_type: _,
                charset: _,
            } => {
                self.format_expr(expr, prefix);
            }

            Expr::Cast {
                expr,
                data_type: _,
                format: _,
            } => {
                self.format_expr(expr, prefix);
            }

            Expr::TryCast {
                expr,
                data_type: _,
                format: _,
            } => {
                self.format_expr(expr, prefix);
            }

            Expr::SafeCast {
                expr,
                data_type: _,
                format: _,
            } => {
                self.format_expr(expr, prefix);
            }

            Expr::Extract { field: _, expr } => {
                self.format_expr(expr, prefix);
            }

            Expr::Ceil { expr, field: _ } => {
                self.format_expr(expr, prefix);
            }

            Expr::Floor { expr, field: _ } => {
                self.format_expr(expr, prefix);
            }
            Expr::Position { expr, r#in } => {
                self.format_expr(expr, prefix);
                self.format_expr(r#in, prefix);
            }

            Expr::Collate { expr, collation: _ } => {
                self.format_expr(expr, prefix);
            }

            Expr::Nested(ast) => {
                self.format_expr(ast, prefix);
            }

            Expr::Value(_v) => {}

            Expr::IntroducedString {
                introducer: _,
                value: _,
            } => {}

            Expr::TypedString {
                data_type: _,
                value: _,
            } => {}

            Expr::Function(fun) => {
                let arg = &mut fun.args;

                for arg in arg.iter_mut() {
                    match arg {
                        FunctionArg::Unnamed(arg) => match arg {
                            FunctionArgExpr::Expr(expr) => {
                                self.format_expr(expr, prefix);
                            }
                            FunctionArgExpr::QualifiedWildcard(_) => {}

                            FunctionArgExpr::Wildcard => {}
                        },
                        FunctionArg::Named { arg, .. } => match arg {
                            FunctionArgExpr::Expr(expr) => {
                                self.format_expr(expr, prefix);
                            }
                            FunctionArgExpr::QualifiedWildcard(_) => {}
                            FunctionArgExpr::Wildcard => {}
                        },
                    }
                }

                if let Some(filter) = &mut fun.filter {
                    self.format_expr(filter, prefix)
                }
            }

            Expr::AggregateExpressionWithFilter { expr, filter } => {
                self.format_expr(expr, prefix);
                self.format_expr(filter, prefix);
            }
            Expr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => {
                if let Some(operand) = operand {
                    self.format_expr(operand, prefix);
                }
                for (c, r) in conditions.iter_mut().zip(results) {
                    self.format_expr(c, prefix);
                    self.format_expr(r, prefix);
                }
                if let Some(else_result) = else_result {
                    self.format_expr(else_result, prefix);
                }
            }

            Expr::Exists {
                subquery,
                negated: _,
            } => {
                self.format_query(subquery, prefix);
            }

            Expr::Subquery(s) => {
                self.format_query(s, prefix);
            }

            Expr::ArraySubquery(s) => {
                self.format_query(s, prefix);
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
                self.format_expr(expr, prefix);
                if let Some(from_part) = substring_from {
                    self.format_expr(from_part, prefix);
                }
                if let Some(for_part) = substring_for {
                    self.format_expr(for_part, prefix);
                }
            }

            Expr::Overlay {
                expr,
                overlay_what,
                overlay_from,
                overlay_for,
            } => {
                self.format_expr(expr, prefix);
                self.format_expr(overlay_what, prefix);
                self.format_expr(overlay_from, prefix);
                if let Some(for_part) = overlay_for {
                    self.format_expr(for_part, prefix);
                }
            }

            Expr::IsDistinctFrom(a, b) | Expr::IsNotDistinctFrom(a, b) => {
                self.format_expr(a, prefix);
                self.format_expr(b, prefix);
            }

            Expr::Trim {
                expr,
                trim_where: _,
                trim_what,
                trim_characters: _,
            } => {
                self.format_expr(expr, prefix);
                if let Some(trim_char) = trim_what {
                    self.format_expr(trim_char, prefix);
                }
            }

            Expr::Tuple(exprs) => {
                for expr in exprs {
                    self.format_expr(expr, prefix);
                }
            }
            
            Expr::Struct { values, fields: _ } => {
                for value in values {
                    self.format_expr(value, prefix);
                }
            }

            Expr::Named { expr, name: _ } => {
                self.format_expr(expr, prefix);
            }

            Expr::ArrayIndex { obj, indexes } => {
                self.format_expr(obj, prefix);
                for i in indexes {
                    self.format_expr(i, prefix);
                }
            }

            Expr::Array(set) => {
                for expr in set.elem.iter_mut() {
                    self.format_expr(expr, prefix);
                }
            }

            Expr::JsonAccess {
                left,
                operator: _,
                right,
            } => {
                self.format_expr(left, prefix);
                self.format_expr(right, prefix);
            }

            Expr::CompositeAccess { expr, key: _ } => {
                self.format_expr(expr, prefix);
            }

            Expr::AtTimeZone {
                timestamp,
                time_zone: _,
            } => {
                self.format_expr(timestamp, prefix);
            }

            Expr::Interval(interval) => self.format_expr(&mut interval.value, prefix),
            Expr::MatchAgainst {
                columns: _,
                match_value: _,
                opt_search_modifier: _,
            } => {}
        }
    }
}
