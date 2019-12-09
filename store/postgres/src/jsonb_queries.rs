///! This module generates queries for the JSONB storage scheme.
///
///  We really only need that for supporting `EntityQuery`
use diesel::pg::Pg;
use diesel::prelude::BoxableExpression;
use diesel::query_builder::{AstPass, Query, QueryFragment, QueryId};
use diesel::query_dsl::RunQueryDsl;
use diesel::result::QueryResult;
use diesel::sql_types::{Array, Bool, Jsonb, Text};
use graph::prelude::{
    EntityCollection, EntityFilter, EntityLink, EntityOrder, EntityRange, EntityWindow,
    QueryExecutionError, ValueType, WindowAttribute,
};

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
    collection: EntityCollection,
    filter: Option<Box<dyn BoxableExpression<EntityTable, Pg, SqlType = Bool>>>,
    order: Option<OrderDetails>,
    range: EntityRange,
}

impl<'a> FilterQuery<'a> {
    pub fn new(
        table: &'a EntityTable,
        collection: EntityCollection,
        filter: Option<EntityFilter>,
        order: Option<(String, ValueType, EntityOrder)>,
        range: EntityRange,
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
            collection,
            filter,
            order,
            range,
        })
    }

    fn entities_clause(&self, entities: &Vec<String>, out: &mut AstPass<Pg>) -> QueryResult<()> {
        if entities.len() == 1 {
            // If there is only one entity_type, which is the case in all
            // queries that do not involve interfaces, leaving out `any`
            // lets Postgres use the primary key index on the entities table
            let entity_type = entities
                .first()
                .expect("we checked that there is exactly one entity_type");
            out.push_sql("entity = ");
            out.push_bind_param::<Text, _>(&entity_type)?;
        } else {
            out.push_sql("entity = any(");
            out.push_bind_param::<Array<Text>, _>(&entities)?;
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

    fn from_window_clause(&self, window: &EntityWindow, out: &mut AstPass<Pg>) -> QueryResult<()> {
        match &window.link {
            EntityLink::Direct(attribute) => {
                match attribute {
                    WindowAttribute::List(name) => {
                        out.push_sql(" join lateral jsonb_array_elements(data->");
                        out.push_bind_param::<Text, _>(name)?;
                        out.push_sql("->'data') parent on parent->>'data' = any(");
                        out.push_bind_param::<Array<Text>, _>(&window.ids)?;
                        out.push_sql(")");
                    }
                    WindowAttribute::Scalar(_) => { /* handled in window_filter */ }
                }
            }
            EntityLink::Parent(parent) => {
                match &parent.child_field {
                    WindowAttribute::Scalar(name) => {
                        // inner join {table} p
                        //   on (p.entity = '{object}'
                        //       and c.id = p.data->{name}->>'data'
                        out.push_sql(" inner join ");
                        self.table.walk_ast(out.reborrow())?;
                        out.push_sql(" p on (p.entity = '");
                        out.push_sql(&parent.parent_type);
                        out.push_sql("' and ");
                        out.push_sql("c.id = p.data->");
                        out.push_bind_param::<Text, _>(name)?;
                        out.push_sql("->>'data'");
                    }
                    WindowAttribute::List(name) => {
                        // p.data->name->'data' is an array where each entry
                        // is a data/type pair. We dissolve that into a table
                        // with only the 'data' values from each array entry
                        //
                        // inner join ({table} p
                        //             join lateral jsonb_array_elements(p.data->{name}->'data') ary(elt) on true) p
                        //   on (p.entity = '{object}'
                        //       and c.id = p.elt->>'data'
                        out.push_sql(" inner join (");
                        self.table.walk_ast(out.reborrow())?;
                        out.push_sql(" p join lateral jsonb_array_elements(p.data->");
                        out.push_bind_param::<Text, _>(name)?;
                        out.push_sql("->'data') ary(elt) on true) p on (p.entity = '");
                        out.push_sql(&parent.parent_type);
                        out.push_sql("' and c.id = p.elt->>'data'");
                    }
                }
                out.push_sql(" and p.id = any(");
                out.push_bind_param::<Array<Text>, _>(&window.ids)?;
                out.push_sql("))");
            }
        }
        Ok(())
    }

    fn parent_id(&self, window: &EntityWindow, out: &mut AstPass<Pg>) -> QueryResult<()> {
        match &window.link {
            EntityLink::Direct(attribute) => match attribute {
                WindowAttribute::Scalar(name) => {
                    out.push_sql("c.data->");
                    out.push_bind_param::<Text, _>(&name)?;
                    out.push_sql("->>'data' as g$parent_id");
                }
                WindowAttribute::List(_) => out.push_sql("parent->>'data' as g$parent_id"),
            },
            EntityLink::Parent(_) => {
                out.push_sql("p.id as g$parent_id");
            }
        }
        Ok(())
    }

    fn window_filter(&self, window: &EntityWindow, out: &mut AstPass<Pg>) -> QueryResult<()> {
        match &window.link {
            EntityLink::Direct(attribute) => {
                match attribute {
                    WindowAttribute::List(_) => { /* handled in from_window_clause */ }
                    WindowAttribute::Scalar(name) => {
                        out.push_sql("\n   and c.data->");
                        out.push_bind_param::<Text, _>(name)?;
                        out.push_sql("->>'data' = any(");
                        out.push_bind_param::<Array<Text>, _>(&window.ids)?;
                        out.push_sql(")");
                    }
                }
            }
            EntityLink::Parent(_) => { /* handled in from_window_clause */ }
        }
        Ok(())
    }

    /// Generate the query when there is no window. This produces
    ///
    ///   select data, entity
    ///     from {table}
    ///    where entity = any({entity_types})
    ///      and {filter}
    ///    order by {order}
    ///    limit {range.first} offset {range.skip}
    fn query_no_window(&self, entities: &Vec<String>, mut out: AstPass<Pg>) -> QueryResult<()> {
        out.unsafe_to_cache_prepared();
        out.push_sql("select id, data, entity");
        out.push_sql("\n  from ");
        self.table.walk_ast(out.reborrow())?;
        out.push_sql(" c\n where ");
        self.entities_clause(entities, &mut out)?;
        if let Some(filter) = &self.filter {
            out.push_sql(" and ");
            filter.walk_ast(out.reborrow())?;
        }

        self.order_by(&mut out)?;
        self.limit(&mut out);
        Ok(())
    }

    /// Generate the query when there is a window. Since we might have
    /// different filters for each entity type, we need to write this as
    /// a `union all` and can't just use `any({entity_types})` as in the
    /// `query_no_window`
    ///
    /// The query we produce is
    ///
    ///   select id, data, entity
    ///     from (select id, data, entity,
    ///                  rank() over (partition by g$parent_id order by {order}) as g$pos
    ///              from {inner_query}) a
    ///    where a.g$pos > {range.skip} and a.g$pos <= {range.skip} + {range.first}
    ///    order by {order}
    ///
    /// And `inner_query` is
    ///   select id, data, entity, {parent_id} as g$parent_id
    ///     from {table}
    ///          [join lateral jsonb_array_elements({window.attribute}) g$parent_id on g$parent_id = any({window.ids})]
    ///    where entity = {entity_type}
    ///      and [{window.attribute} = any({window.ids})]
    ///      and {filter}
    ///    union all
    ///      ...
    /// where we loop this over every entry in `self.window`
    ///
    /// The `join lateral` clause is only needed if the `window.attribute` is a list; if it is
    /// a scalar, we use the `[{window.attribute} = any({window.ids})]` clause instead
    fn query_window(&self, windows: &Vec<EntityWindow>, mut out: AstPass<Pg>) -> QueryResult<()> {
        out.unsafe_to_cache_prepared();
        out.push_sql(
            "select id, data || jsonb_build_object('g$parent_id', jsonb_build_object('data', g$parent_id, 'type', 'String')), entity\n  from (",
        );

        out.push_sql("select id, data, entity, g$parent_id");
        out.push_sql(", rank() over (partition by g$parent_id");
        self.order_by(&mut out)?;
        out.push_sql(") as g$pos");
        out.push_sql("\n  from (");
        // inner_query starts here
        for (index, window) in windows.iter().enumerate() {
            if index > 0 {
                out.push_sql("\nunion all\n");
            }
            out.push_sql("select c.id, c.data, c.entity, ");
            self.parent_id(window, &mut out)?;
            out.push_sql("\n  from ");
            self.table.walk_ast(out.reborrow())?;
            out.push_sql(" c");
            self.from_window_clause(window, &mut out)?;
            out.push_sql("\n where c.entity = ");
            out.push_bind_param::<Text, _>(&window.child_type)?;
            self.window_filter(window, &mut out)?;
            if let Some(filter) = &self.filter {
                out.push_sql("\n   and ");
                filter.walk_ast(out.reborrow())?;
            }
        }
        // back to the outer query
        out.push_sql(") a) a\n where ");
        if self.range.skip > 0 {
            out.push_sql("a.g$pos > ");
            out.push_sql(&self.range.skip.to_string());
            if self.range.first.is_some() {
                out.push_sql(" and ");
            }
        }
        if let Some(first) = self.range.first {
            let pos = self.range.skip + first;
            out.push_sql("a.g$pos <= ");
            out.push_sql(&pos.to_string());
        }
        out.push_sql("\n order by a.g$parent_id, a.g$pos");
        Ok(())
    }
}

impl<'a> QueryFragment<Pg> for FilterQuery<'a> {
    fn walk_ast(&self, out: AstPass<Pg>) -> QueryResult<()> {
        match &self.collection {
            EntityCollection::All(entities) => self.query_no_window(entities, out),
            EntityCollection::Window(windows) => self.query_window(windows, out),
        }
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
