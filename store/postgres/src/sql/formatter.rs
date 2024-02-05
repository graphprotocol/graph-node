use sqlparser::ast::{Ident, ObjectName, Statement, TableFactor, Visit, VisitMut, VisitorMut};
use std::ops::ControlFlow;

pub struct Formatter<'a> {
    namespace: &'a str,
    cte_prefix: &'a str,
}

impl<'a> Formatter<'a> {
    pub fn new(namespace: &'a str, cte_prefix: &'a str) -> Self {
        Self {
            namespace,
            cte_prefix,
        }
    }

    fn prepend_prefix_to_object_name_mut(&self, name: &mut ObjectName) {
        name.0.insert(0, Ident::new(self.namespace));
    }

    pub fn format(&mut self, statement: &mut Statement) -> String {
        statement.visit(self);

        format!(
            "{} SELECT to_jsonb(sub.*) AS data FROM ( {} ) AS sub;",
            self.cte_prefix, statement
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
    use crate::sql::constants::SQL_DIALECT;
    const NAMESPACE: &str = "sgd0815";
    const CTE_PREFIX: &str = "-- CTE --";

    #[test]
    fn format_sql() {
        let mut formatter = Formatter::new(NAMESPACE, CTE_PREFIX);

        let sql = "SELECT * FROM swap";

        let mut statements = sqlparser::parser::Parser::parse_sql(&SQL_DIALECT, sql).unwrap();

        let mut statement = statements.get_mut(0).unwrap();

        let result = formatter.format(&mut statement);

        assert_eq!(
            result,
            format!(
                "{} SELECT to_jsonb(sub.*) AS data FROM ( {} ) AS sub;",
                CTE_PREFIX,
                format!("SELECT * FROM {}.swap", NAMESPACE)
            )
        );
    }
}
