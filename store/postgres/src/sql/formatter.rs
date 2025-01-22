use sqlparser::ast::{ObjectName, Statement, TableFactor, VisitMut, VisitorMut};
use std::ops::ControlFlow;

use crate::relational::{Layout, SqlName};

pub struct Formatter<'a> {
    prelude: &'a str,
    layout: &'a Layout,
}

impl<'a> Formatter<'a> {
    pub fn new(prelude: &'a str, layout: &'a Layout) -> Self {
        Self { prelude, layout }
    }

    fn prepend_prefix_to_object_name_mut(&self, name: &mut ObjectName) {
        let table_identifier = &mut name.0;
        // remove all but the last identifier
        table_identifier.drain(0..table_identifier.len() - 1);

        // Ensure schema tables has quotation to match up with prelude generated cte.
        if let Some(table_name) = table_identifier.last_mut() {
            let sql_name = SqlName::verbatim(table_name.to_string());
            if self.layout.table(&sql_name).is_some() {
                table_name.quote_style = Some('"');
            }
        }
    }

    pub fn format(&mut self, statement: &mut Statement) -> String {
        statement.visit(self);

        format!(
            "{} SELECT to_jsonb(sub.*) AS data FROM ( {} ) AS sub",
            self.prelude, statement
        )
    }
}

impl VisitorMut for Formatter<'_> {
    type Break = ();

    fn pre_visit_table_factor(
        &mut self,
        table_factor: &mut TableFactor,
    ) -> ControlFlow<Self::Break> {
        if let TableFactor::Table { name, .. } = table_factor {
            self.prepend_prefix_to_object_name_mut(name);
        }
        ControlFlow::Continue(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::sql::{constants::SQL_DIALECT, test::make_layout};

    const GQL: &str = "
    type Swap @entity {
        id: ID!
        amountIn: BigDecimal!
        amountOut: BigDecimal!
        tokenIn: Bytes!
        tokenOut: Bytes!
    }
    ";

    const CTE_PREFIX: &str = "WITH \"swap\" AS (
            SELECT
            id,
            amount_in,
            amount_out,
            concat('0x',encode(token_in,'hex') as token_in,
            concat('0x',token_out,'hex') AS token_out
            FROM
            sdg1.swap
        )";

    #[test]
    fn format_sql() {
        let layout = make_layout(GQL);

        let mut formatter = Formatter::new(CTE_PREFIX, &layout);

        let sql = "SELECT token_in, SUM(amount_in) AS amount FROM unknown.swap GROUP BY token_in";

        let mut statements = sqlparser::parser::Parser::parse_sql(&SQL_DIALECT, sql).unwrap();

        let mut statement = statements.get_mut(0).unwrap();

        let result = formatter.format(&mut statement);

        assert_eq!(
            result,
            format!(
                "{} SELECT to_jsonb(sub.*) AS data FROM ( {} ) AS sub",
                CTE_PREFIX,
                "SELECT token_in, SUM(amount_in) AS amount FROM \"swap\" GROUP BY token_in"
            )
        );
    }
}
