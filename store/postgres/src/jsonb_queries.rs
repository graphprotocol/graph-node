///! This module generates queries for the JSONB storage scheme.
///
///  We really only need that for supporting `EntityQuery`
use diesel::pg::Pg;
use diesel::prelude::BoxableExpression;
use diesel::query_builder::{AstPass, Query, QueryFragment, QueryId};
use diesel::query_dsl::RunQueryDsl;
use diesel::result::QueryResult;
use diesel::sql_types::{Array, Bool, Jsonb, Text};

use graph::prelude::{EntityFilter, EntityOrder, EntityRange, QueryExecutionError, ValueType};

use crate::entities::{EntityTable, STRING_PREFIX_SIZE};
use crate::filter::build_filter;
use crate::relational::PRIMARY_KEY_COLUMN;

pub struct OrderDetails {
    attribute: String,
    cast: &'static str,
    prefix_only: bool,
    direction: EntityOrder,
}

pub struct FilterQuery<'a> {
    table: &'a EntityTable,
    entity_types: Vec<String>,
    filter: Option<Box<dyn BoxableExpression<EntityTable, Pg, SqlType = Bool>>>,
    order: Option<OrderDetails>,
    range: EntityRange,
    window: Option<String>,
}

impl<'a> FilterQuery<'a> {
    pub fn new(
        table: &'a EntityTable,
        entity_types: Vec<String>,
        filter: Option<EntityFilter>,
        order: Option<(String, ValueType, EntityOrder)>,
        range: EntityRange,
        window: Option<String>,
    ) -> Result<Self, QueryExecutionError> {
        let order = if let Some((attribute, value_type, direction)) = order {
            let cast = match value_type {
                ValueType::BigInt | ValueType::BigDecimal => "::numeric",
                ValueType::Boolean => "::boolean",
                ValueType::Bytes => "",
                ValueType::ID => "",
                ValueType::Int => "::bigint",
                ValueType::String => "",
                ValueType::List => {
                    return Err(QueryExecutionError::OrderByNotSupportedForType(
                        "List".to_string(),
                    ));
                }
            };

            let prefix_only = &attribute != PRIMARY_KEY_COLUMN && value_type == ValueType::String;
            Some(OrderDetails {
                attribute,
                cast,
                prefix_only,
                direction,
            })
        } else {
            None
        };
        let filter = if let Some(filter) = filter {
            Some(build_filter(filter).map_err(|e| {
                QueryExecutionError::FilterNotSupportedError(format!("{}", e.value), e.filter)
            })?)
        } else {
            None
        };
        Ok(FilterQuery {
            table,
            entity_types,
            filter,
            order,
            range,
            window,
        })
    }

    fn entity_types(&self, out: &mut AstPass<Pg>) -> QueryResult<()> {
        if self.entity_types.len() == 1 {
            // If there is only one entity_type, which is the case in all
            // queries that do not involve interfaces, leaving out `any`
            // lets Postgres use the primary key index on the entities table
            let entity_type = self
                .entity_types
                .first()
                .expect("we checked that there is exactly one entity_type");
            out.push_sql("entity = ");
            out.push_bind_param::<Text, _>(&entity_type)?;
        } else {
            out.push_sql("entity = any(");
            out.push_bind_param::<Array<Text>, _>(&self.entity_types)?;
            out.push_sql(")");
        }
        Ok(())
    }

    fn order_by(&self, out: &mut AstPass<Pg>) -> QueryResult<()> {
        out.push_sql("\n order by ");
        if let Some(order) = &self.order {
            if order.prefix_only {
                out.push_sql("left(data ->");
                out.push_bind_param::<Text, _>(&order.attribute)?;
                out.push_sql("->> 'data', ");
                out.push_sql(&STRING_PREFIX_SIZE.to_string());
                out.push_sql(") ");
            } else {
                if &order.attribute == PRIMARY_KEY_COLUMN {
                    out.push_identifier(PRIMARY_KEY_COLUMN)?;
                } else {
                    out.push_sql("(data ->");
                    out.push_bind_param::<Text, _>(&order.attribute)?;
                    out.push_sql("->> 'data')");
                    out.push_sql(&order.cast);
                }
                out.push_sql(" ");
            }
            out.push_sql(order.direction.to_sql());
            out.push_sql(" nulls last, ");
        }
        out.push_identifier(PRIMARY_KEY_COLUMN)
    }

    fn limit(&self, out: &mut AstPass<Pg>) {
        if let Some(first) = &self.range.first {
            out.push_sql("\n limit ");
            out.push_sql(&first.to_string());
        }
        if self.range.skip > 0 {
            out.push_sql("\noffset ");
            out.push_sql(&self.range.skip.to_string());
        }
    }
}

impl<'a> QueryFragment<Pg> for FilterQuery<'a> {
    fn walk_ast(&self, mut out: AstPass<Pg>) -> QueryResult<()> {
        // We generate two somewhat different queries depending on whether
        // we need to window over a field or not. Without a window,
        // we generate
        //    select data, entity
        //      from {table}
        //     where entity = any({entity_types})
        //       and {filter}
        //     order by {order}
        //     limit {range.first} offset {range.skip}
        //
        // When there is a window, we generate a query
        //     select data, entity
        //       from (select data, entity,
        //                    rank() over (partition by {window} order by {order}) as pos
        //                from {inner_query}) a
        //      where a.pos > {range.skip} and a.pos <= {range.skip} + {range.first}
        //      order by {order}
        // Here, inner_query are the corresponding parts from the first query,
        // but without the order by or limit clauses
        out.unsafe_to_cache_prepared();
        if self.window.is_some() {
            out.push_sql("select id, data, entity\n  from (");
        }

        out.push_sql("select id, data, entity");
        if let Some(window) = &self.window {
            out.push_sql(", rank() over (partition by data->");
            out.push_bind_param::<Text, _>(window)?;
            out.push_sql("->>'data' ");
            self.order_by(&mut out)?;
            out.push_sql(") as pos");
        }
        out.push_sql("\n  from ");
        self.table.walk_ast(out.reborrow())?;
        out.push_sql(" \n where ");
        self.entity_types(&mut out)?;
        if let Some(filter) = &self.filter {
            out.push_sql(" and ");
            filter.walk_ast(out.reborrow())?;
        }

        if self.window.is_some() {
            out.push_sql(") a\n where ");
            if self.range.skip > 0 {
                out.push_sql("a.pos > ");
                out.push_sql(&self.range.skip.to_string());
                if self.range.first.is_some() {
                    out.push_sql(" and ");
                }
            }
            if let Some(first) = self.range.first {
                let pos = self.range.skip + first;
                out.push_sql("a.pos <= ");
                out.push_sql(&pos.to_string());
            }
            self.order_by(&mut out)?;
        } else {
            self.order_by(&mut out)?;
            self.limit(&mut out);
        }
        Ok(())
    }
}

impl<'a> QueryId for FilterQuery<'a> {
    type QueryId = ();

    const HAS_STATIC_QUERY_ID: bool = false;
}

impl<'a> Query for FilterQuery<'a> {
    type SqlType = (Text, Jsonb, Text);
}

impl<'a, Conn> RunQueryDsl<Conn> for FilterQuery<'a> {}
