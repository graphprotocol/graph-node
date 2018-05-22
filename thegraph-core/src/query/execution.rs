use graphql_parser::query as gqlq;
use graphql_parser::schema as gqls;
use indexmap::IndexMap;
use slog;
use std::cmp;
use std::collections::BTreeMap;

use thegraph::prelude::*;

use super::ast as qast;
use schema::ast as sast;

/// A GraphQL resolver that can resolve entities, enum values, scalar types and interfaces/unions.
pub trait Resolver: Clone {
    /// Resolves entities referenced by a parent object.
    fn resolve_entities(
        &self,
        parent: &Option<gqlq::Value>,
        entity: &String,
        arguments: &Vec<&(gqlq::Name, gqlq::Value)>,
    ) -> gqlq::Value;

    /// Resolves an entity referenced by a parent object.
    fn resolve_entity(
        &self,
        parent: &Option<gqlq::Value>,
        entity: &String,
        arguments: &Vec<&(gqlq::Name, gqlq::Value)>,
    ) -> gqlq::Value;

    /// Resolves an enum value for a given enum type.
    fn resolve_enum_value(
        &self,
        enum_type: &gqls::EnumType,
        value: Option<&gqlq::Value>,
    ) -> gqlq::Value;

    /// Resolves a scalar value for a given scalar type.
    fn resolve_scalar_value(
        &self,
        scalar_type: &gqls::ScalarType,
        value: Option<&gqlq::Value>,
    ) -> gqlq::Value;

    /// Resolves a list of enum values for a given enum type.
    fn resolve_enum_values(
        &self,
        enum_type: &gqls::EnumType,
        value: Option<&gqlq::Value>,
    ) -> gqlq::Value;

    /// Resolves a list of scalar values for a given list type.
    fn resolve_scalar_values(
        &self,
        scalar_type: &gqls::ScalarType,
        value: Option<&gqlq::Value>,
    ) -> gqlq::Value;

    // Resolves an abstract type into the specific type of an object.
    fn resolve_abstract_type<'a>(
        &self,
        schema: &'a gqls::Document,
        abstract_type: &gqls::TypeDefinition,
        object_value: &gqlq::Value,
    ) -> Option<&'a gqls::ObjectType>;
}

/// Contextual information passed around during query execution.
#[derive(Clone)]
struct ExecutionContext<'a, R>
where
    R: Resolver,
{
    /// The logger to use.
    pub logger: slog::Logger,
    /// The schema to execute the query against.
    pub schema: &'a Schema,
    /// The query to execute.
    pub query: &'a Query,
    /// The resolver to use.
    pub resolver: R,
    /// The current field stack (e.g. allUsers > friends > name).
    pub fields: Vec<&'a gqlq::Field>,
}

impl<'a, R> ExecutionContext<'a, R>
where
    R: Resolver,
{
    /// Creates a derived context for a new field (added to the top of the field stack).
    pub fn for_field(&mut self, field: &'a gqlq::Field) -> Self {
        let mut ctx = self.clone();
        ctx.fields.push(field);
        ctx
    }
}

/// Optionsp available for the `execute` function.
pub struct ExecutionOptions<R>
where
    R: Resolver,
{
    /// The logger to use during query execution.
    pub logger: slog::Logger,
    /// The resolver to use.
    pub resolver: R,
}

/// Executes a query and returns a result.
pub fn execute<R>(query: &Query, options: ExecutionOptions<R>) -> QueryResult
where
    R: Resolver,
{
    info!(options.logger, "Execute");

    // Obtain the only operation of the query (fail if there is none or more than one)
    let operation = match qast::get_operation(&query.document, None) {
        Ok(op) => op,
        Err(e) => return QueryResult::from(e),
    };

    // Create a fresh execution context
    let ctx = ExecutionContext {
        logger: options.logger,
        resolver: options.resolver,
        schema: &query.schema,
        query,
        fields: vec![],
    };

    match operation {
        // Execute top-level `query { ... }` expressions
        &gqlq::OperationDefinition::Query(gqlq::Query {
            ref selection_set, ..
        }) => execute_root_selection_set(ctx, selection_set, &None),

        // Execute top-level `{ ... }` expressions
        &gqlq::OperationDefinition::SelectionSet(ref selection_set) => {
            execute_root_selection_set(ctx, selection_set, &None)
        }

        // Everything else (e.g. mutations) is unsupported
        _ => QueryResult::from(QueryExecutionError::NotSupported(
            "Only queries are supported".to_string(),
        )),
    }
}

/// Executes the root selection set of a query.
fn execute_root_selection_set<'a, R>(
    ctx: ExecutionContext<'a, R>,
    selection_set: &'a gqlq::SelectionSet,
    initial_value: &Option<gqlq::Value>,
) -> QueryResult
where
    R: Resolver,
{
    // Obtain the root Query type and fail if there isn't one
    let query_type = match sast::get_root_query_type(&ctx.schema.document) {
        Some(t) => t,
        None => return QueryResult::from(QueryExecutionError::NoRootQueryObjectType),
    };

    // Execute the root selection set against the root query type
    execute_selection_set(ctx, selection_set, query_type, initial_value)
        .unwrap_or_else(QueryResult::from)
}

/// Executes a selection set, requiring the result to be of the given object type.
///
/// Allows passing in a parent value during recursive processing of objects and their fields.
fn execute_selection_set<'a, R>(
    mut ctx: ExecutionContext<'a, R>,
    selection_set: &'a gqlq::SelectionSet,
    object_type: &gqls::ObjectType,
    object_value: &Option<gqlq::Value>,
) -> Result<QueryResult, QueryExecutionError>
where
    R: Resolver,
{
    let mut result = QueryResult::new(None);
    let mut result_map: BTreeMap<String, gqlq::Value> = BTreeMap::new();

    // Group fields with the same response key, so we can execute them together
    let grouped_field_set = collect_fields(ctx.clone(), object_type, selection_set);

    // Process all field groups in order
    for (response_key, fields) in grouped_field_set {
        // If the field exists on the object, execute it and add its result to the result map
        if let Some(ref field) = sast::get_field_type(object_type, &fields[0].name) {
            let ctx = ctx.for_field(&fields[0]);

            match execute_field(
                ctx,
                object_type,
                object_value,
                &fields[0],
                &field.field_type,
                fields,
            ) {
                Ok(v) => {
                    result_map.insert(response_key.to_owned(), v);
                }
                Err(e) => {
                    result.add_error(QueryError::from(e));
                }
            };
        }
    }

    // If we have result data, wrap it in an output object
    if !result_map.is_empty() {
        result.data = Some(gqlq::Value::Object(result_map));
    }

    Ok(result)
}

/// Collects fields of a selection set.
fn collect_fields<'a, R>(
    _ctx: ExecutionContext<'a, R>,
    _object_type: &gqls::ObjectType,
    selection_set: &'a gqlq::SelectionSet,
) -> IndexMap<&'a String, Vec<&'a gqlq::Field>>
where
    R: Resolver,
{
    let mut grouped_fields = IndexMap::new();

    // Only consider selections that are not skipped and should be included
    let selections: Vec<_> = selection_set
        .items
        .iter()
        .filter(|selection| !qast::skip_selection(selection))
        .filter(|selection| qast::include_selection(selection))
        .collect();

    for selection in selections {
        match selection {
            gqlq::Selection::Field(ref field) => {
                // Obtain the response key for the field
                let response_key = qast::get_response_key(field);

                // Create a field group for this response key on demand
                if !grouped_fields.contains_key(response_key) {
                    grouped_fields.insert(response_key, vec![]);
                }

                // Append the selection field to this group
                let mut group = grouped_fields.get_mut(response_key).unwrap();
                group.push(field);
            }

            gqlq::Selection::FragmentSpread(_) => unimplemented!(),
            gqlq::Selection::InlineFragment(_) => unimplemented!(),
        };
    }

    grouped_fields
}

/// Executes a field.
fn execute_field<'a, R>(
    ctx: ExecutionContext<'a, R>,
    object_type: &gqls::ObjectType,
    object_value: &Option<gqlq::Value>,
    field: &'a gqlq::Field,
    field_type: &'a gqls::Type,
    fields: Vec<&'a gqlq::Field>,
) -> Result<gqlq::Value, QueryExecutionError>
where
    R: Resolver,
{
    resolve_field_value(ctx.clone(), object_type, object_value, field, field_type)
        .and_then(|value| complete_value(ctx, field, field_type, fields, value))
}

/// Resolves the value of a field.
fn resolve_field_value<'a, R>(
    ctx: ExecutionContext<'a, R>,
    object_type: &gqls::ObjectType,
    object_value: &Option<gqlq::Value>,
    field: &gqlq::Field,
    field_type: &gqls::Type,
) -> Result<gqlq::Value, QueryExecutionError>
where
    R: Resolver,
{
    match field_type {
        gqls::Type::NonNullType(inner_type) => {
            resolve_field_value(ctx, object_type, object_value, field, inner_type.as_ref())
        }

        gqls::Type::NamedType(ref name) => {
            resolve_field_value_for_named_type(ctx, object_value, field, name)
        }

        gqls::Type::ListType(inner_type) => resolve_field_value_for_list_type(
            ctx,
            object_type,
            object_value,
            field,
            field_type,
            inner_type.as_ref(),
        ),
    }
}

/// Resolves the value of a field that corresponds to a named type.
fn resolve_field_value_for_named_type<'a, R>(
    ctx: ExecutionContext<'a, R>,
    object_value: &Option<gqlq::Value>,
    field: &gqlq::Field,
    type_name: &gqls::Name,
) -> Result<gqlq::Value, QueryExecutionError>
where
    R: Resolver,
{
    // Try to resolve the type name into the actual type
    let named_type = sast::get_named_type(&ctx.schema.document, type_name)
        .ok_or(QueryExecutionError::NamedTypeError(type_name.to_string()))?;

    match named_type {
        // Let the resolver decide how the field (with the given object type)
        // is resolved into an entity based on the (potential) parent object
        gqls::TypeDefinition::Object(t) => {
            Ok(ctx.resolver.resolve_entity(object_value, &t.name, &vec![]))
        }

        // Let the resolver decide how values in the resolved object value
        // map to values of GraphQL enums
        gqls::TypeDefinition::Enum(t) => match object_value {
            Some(gqlq::Value::Object(o)) => {
                Ok(ctx.resolver.resolve_enum_value(t, o.get(&field.name)))
            }
            _ => Ok(gqlq::Value::Null),
        },

        // Let the resolver decide how values in the resolved object value
        // map to values of GraphQL scalars
        gqls::TypeDefinition::Scalar(t) => match object_value {
            Some(gqlq::Value::Object(o)) => {
                Ok(ctx.resolver.resolve_scalar_value(t, o.get(&field.name)))
            }
            _ => Ok(gqlq::Value::Null),
        },

        // We will implement these later
        gqls::TypeDefinition::Interface(_) => unimplemented!(),
        gqls::TypeDefinition::Union(_) => unimplemented!(),

        _ => unimplemented!(),
    }
}

/// Resolves the value of a field that corresponds to a list type.
fn resolve_field_value_for_list_type<'a, R>(
    ctx: ExecutionContext<'a, R>,
    object_type: &gqls::ObjectType,
    object_value: &Option<gqlq::Value>,
    field: &gqlq::Field,
    field_type: &gqls::Type,
    inner_type: &gqls::Type,
) -> Result<gqlq::Value, QueryExecutionError>
where
    R: Resolver,
{
    match inner_type {
        gqls::Type::NonNullType(inner_type) => resolve_field_value_for_list_type(
            ctx,
            object_type,
            object_value,
            field,
            field_type,
            inner_type,
        ),

        gqls::Type::NamedType(ref type_name) => {
            let named_type = sast::get_named_type(&ctx.schema.document, type_name).unwrap();

            match named_type {
                // Let the resolver decide how the list field (with the given item object type)
                // is resolved into a entities based on the (potential) parent object
                gqls::TypeDefinition::Object(t) => {
                    Ok(ctx.resolver
                        .resolve_entities(object_value, &t.name, &vec![]))
                }

                // Let the resolver decide how values in the resolved object value
                // map to values of GraphQL enums
                gqls::TypeDefinition::Enum(t) => match object_value {
                    Some(gqlq::Value::Object(o)) => {
                        Ok(ctx.resolver.resolve_enum_values(t, o.get(&field.name)))
                    }
                    _ => Ok(gqlq::Value::Null),
                },

                // Let the resolver decide how values in the resolved object value
                // map to values of GraphQL scalars
                gqls::TypeDefinition::Scalar(t) => match object_value {
                    Some(gqlq::Value::Object(o)) => {
                        Ok(ctx.resolver.resolve_scalar_values(t, o.get(&field.name)))
                    }
                    _ => Ok(gqlq::Value::Null),
                },

                // We will implement these later
                gqls::TypeDefinition::Interface(_) => unimplemented!(),
                gqls::TypeDefinition::Union(_) => unimplemented!(),

                _ => unimplemented!(),
            }
        }

        // We don't support nested lists yet
        gqls::Type::ListType(_) => unimplemented!(),
    }
}

/// Ensures that a value matches the expected return type.
fn complete_value<'a, R>(
    ctx: ExecutionContext<'a, R>,
    field: &'a gqlq::Field,
    field_type: &'a gqls::Type,
    fields: Vec<&'a gqlq::Field>,
    resolved_value: gqlq::Value,
) -> Result<gqlq::Value, QueryExecutionError>
where
    R: Resolver,
{
    // Fail if the field type is non-null but the value is null
    if let gqls::Type::NonNullType(inner_type) = field_type {
        return match complete_value(ctx.clone(), field, inner_type, fields, resolved_value)? {
            gqlq::Value::Null => Err(QueryExecutionError::NonNullError(
                field.position,
                field.name.to_string(),
            )),

            v => Ok(v),
        };
    }

    // If the resolved value is null, return null
    if resolved_value == gqlq::Value::Null {
        return Ok(resolved_value);
    }

    // Complete list values
    if let gqls::Type::ListType(inner_type) = field_type {
        return match resolved_value {
            // Complete list values individually
            gqlq::Value::List(values) => {
                let mut out = Vec::with_capacity(values.len());
                for value in values.into_iter() {
                    out.push(complete_value(
                        ctx.clone(),
                        field,
                        inner_type,
                        fields.clone(),
                        value,
                    )?);
                }
                Ok(gqlq::Value::List(out))
            }

            // Return field error if the resolved value for the list is not a list
            _ => Err(QueryExecutionError::ListValueError(
                field.position,
                field.name.to_string(),
            )),
        };
    }

    let named_type = if let gqls::Type::NamedType(name) = field_type {
        Some(sast::get_named_type(&ctx.schema.document, name).unwrap())
    } else {
        None
    };

    match named_type {
        // Complete scalar values; we're assuming that the resolver has
        // already returned a valid value for the scalar type
        Some(gqls::TypeDefinition::Scalar(_)) => return Ok(resolved_value),

        // Complete enum values; we're assuming that the resolver has
        // already returned a valid value for the enum type
        Some(gqls::TypeDefinition::Enum(_)) => return Ok(resolved_value),

        // Complete object types recursively
        Some(gqls::TypeDefinition::Object(object_type)) => {
            execute_selection_set(
                ctx.clone(),
                &merge_selection_sets(fields),
                object_type,
                &Some(resolved_value),
            ).map(|result| match result.data {
                Some(v) => v,
                None => gqlq::Value::Null,
            })
        }

        // Resolve interface types using the resolved value and complete the value recursively
        Some(gqls::TypeDefinition::Interface(_)) => {
            let object_type =
                resolve_abstract_type(ctx.clone(), named_type.unwrap(), &resolved_value)?;

            execute_selection_set(
                ctx.clone(),
                &merge_selection_sets(fields),
                object_type,
                &Some(resolved_value),
            ).map(|result| match result.data {
                Some(v) => v,
                None => gqlq::Value::Null,
            })
        }

        // Resolve union types using the resolved value and complete the value recursively
        Some(gqls::TypeDefinition::Union(_)) => {
            let object_type =
                resolve_abstract_type(ctx.clone(), named_type.unwrap(), &resolved_value)?;

            execute_selection_set(
                ctx.clone(),
                &merge_selection_sets(fields),
                object_type,
                &Some(resolved_value),
            ).map(|result| match result.data {
                Some(v) => v,
                None => gqlq::Value::Null,
            })
        }

        _ => unimplemented!(),
    }
}

/// Resolves an abstract type (interface, union) into an object type based on the given value.
fn resolve_abstract_type<'a, R>(
    ctx: ExecutionContext<'a, R>,
    abstract_type: &'a gqls::TypeDefinition,
    object_value: &gqlq::Value,
) -> Result<&'a gqls::ObjectType, QueryExecutionError>
where
    R: Resolver,
{
    // Let the resolver handle the type resolution, return an error if the resolution
    // yields nothing
    ctx.resolver
        .resolve_abstract_type(&ctx.schema.document, abstract_type, object_value)
        .ok_or(QueryExecutionError::AbstractTypeError(
            sast::get_type_name(abstract_type).to_string(),
        ))
}

/// Merges the selection sets of several fields into a single selection set.
fn merge_selection_sets(fields: Vec<&gqlq::Field>) -> gqlq::SelectionSet {
    let (span, items) = fields
        .iter()
        .fold((None, vec![]), |(span, mut items), field| {
            (
                // The overal span is the min/max spans of all merged selection sets
                match span {
                    None => Some(field.selection_set.span.clone()),
                    Some((start, end)) => Some((
                        cmp::min(start, field.selection_set.span.0),
                        cmp::max(end, field.selection_set.span.1),
                    )),
                },
                // The overall selection is the result of merging the selections of all fields
                {
                    items.extend_from_slice(field.selection_set.items.as_slice());
                    items
                },
            )
        });

    gqlq::SelectionSet {
        span: span.unwrap(),
        items,
    }
}
