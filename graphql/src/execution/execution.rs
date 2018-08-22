use graphql_parser::query as q;
use graphql_parser::schema as s;
use indexmap::IndexMap;
use std::cmp;
use std::collections::{BTreeMap, HashMap, HashSet};

use graph::prelude::*;

use prelude::*;
use query::ast as qast;
use schema::ast as sast;

/// Contextual information passed around during query execution.
#[derive(Clone)]
pub struct ExecutionContext<'a, R1, R2>
where
    R1: Resolver,
    R2: Resolver,
{
    /// The logger to use.
    pub logger: slog::Logger,
    /// The schema to execute the query against.
    pub schema: &'a Schema,
    /// Introspection data that corresponds to the schema.
    pub introspection_schema: &'a s::Document,
    /// The query to execute.
    pub document: &'a q::Document,
    /// The resolver to use.
    pub resolver: Arc<R1>,
    /// The introspection resolver to use.
    pub introspection_resolver: Arc<R2>,
    /// The current field stack (e.g. allUsers > friends > name).
    pub fields: Vec<&'a q::Field>,
    /// Whether or not we're executing an introspection query
    pub introspecting: bool,
}

impl<'a, R1, R2> ExecutionContext<'a, R1, R2>
where
    R1: Resolver,
    R2: Resolver,
{
    /// Creates a derived context for a new field (added to the top of the field stack).
    pub fn for_field(&mut self, field: &'a q::Field) -> Self {
        let mut ctx = self.clone();
        ctx.fields.push(field);
        ctx
    }
}

/// Executes the root selection set of a query.
pub fn execute_root_selection_set<'a, R1, R2>(
    ctx: ExecutionContext<'a, R1, R2>,
    selection_set: &'a q::SelectionSet,
    initial_value: &Option<q::Value>,
) -> QueryResult
where
    R1: Resolver,
    R2: Resolver,
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
pub fn execute_selection_set<'a, R1, R2>(
    mut ctx: ExecutionContext<'a, R1, R2>,
    selection_set: &'a q::SelectionSet,
    object_type: &s::ObjectType,
    object_value: &Option<q::Value>,
) -> Result<QueryResult, QueryExecutionError>
where
    R1: Resolver,
    R2: Resolver,
{
    let mut result = QueryResult::new(None);
    let mut result_map: BTreeMap<String, q::Value> = BTreeMap::new();

    // Group fields with the same response key, so we can execute them together
    let grouped_field_set = collect_fields(ctx.clone(), object_type, selection_set, None);

    // Process all field groups in order
    for (response_key, fields) in grouped_field_set {
        // If the field exists on the object, execute it and add its result to the result map
        if let Some((ref field, introspecting)) =
            get_field_type(ctx.clone(), object_type, &fields[0].name)
        {
            // Push the new field onto the context's field stack
            let mut ctx = ctx.for_field(&fields[0]);

            // Remember whether or not we're introspecting now
            ctx.introspecting = introspecting;

            match execute_field(ctx, object_type, object_value, &fields[0], field, fields) {
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
        result.data = Some(q::Value::Object(result_map));
    }

    Ok(result)
}

/// Collects fields of a selection set.
pub fn collect_fields<'a, R1, R2>(
    ctx: ExecutionContext<'a, R1, R2>,
    object_type: &s::ObjectType,
    selection_set: &'a q::SelectionSet,
    visited_fragments: Option<HashSet<&'a q::Name>>,
) -> IndexMap<&'a String, Vec<&'a q::Field>>
where
    R1: Resolver,
    R2: Resolver,
{
    let mut visited_fragments = visited_fragments.unwrap_or(HashSet::new());
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
            q::Selection::Field(ref field) => {
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

            q::Selection::FragmentSpread(spread) => {
                // Only consider the fragment if it hasn't already been included,
                // as would be the case if the same fragment spread ...Foo appeared
                // twice in the same selection set
                if !visited_fragments.contains(&spread.fragment_name) {
                    visited_fragments.insert(&spread.fragment_name);

                    // Resolve the fragment using its name and, if it applies, collect
                    // fields for the fragment and group them
                    let fragment_grouped_field_set =
                        qast::get_fragment(&ctx.document, &spread.fragment_name)
                            .and_then(|fragment| {
                                // We have a fragment, only pass it on if it applies to the
                                // current object type
                                if does_fragment_type_apply(
                                    ctx.clone(),
                                    object_type,
                                    &fragment.type_condition,
                                ) {
                                    Some(fragment)
                                } else {
                                    None
                                }
                            })
                            .map(|fragment| {
                                // We have a fragment that applies to the current object type,
                                // collect its fields into response key groups
                                collect_fields(
                                    ctx.clone(),
                                    object_type,
                                    &fragment.selection_set,
                                    Some(visited_fragments.clone()),
                                )
                            });

                    if let Some(grouped_field_set) = fragment_grouped_field_set {
                        // Add all items from each fragments group to the field group
                        // with the corresponding response key
                        for (response_key, mut fragment_group) in grouped_field_set {
                            if !grouped_fields.contains_key(response_key) {
                                grouped_fields.insert(response_key, vec![]);
                            }
                            let mut group = grouped_fields.get_mut(response_key).unwrap();
                            group.append(&mut fragment_group);
                        }
                    }
                }
            }

            q::Selection::InlineFragment(_) => unimplemented!(),
        };
    }

    grouped_fields
}

/// Determines whether a fragment is applicable to the given object type.
fn does_fragment_type_apply<'a, R1, R2>(
    ctx: ExecutionContext<'a, R1, R2>,
    object_type: &s::ObjectType,
    fragment_type: &q::TypeCondition,
) -> bool
where
    R1: Resolver,
    R2: Resolver,
{
    // This is safe to do, as TypeCondition only has a single `On` variant.
    let q::TypeCondition::On(ref name) = fragment_type;

    // Resolve the type the fragment applies to based on its name
    let named_type = sast::get_named_type(
        if ctx.introspecting {
            ctx.introspection_schema
        } else {
            &ctx.schema.document
        },
        name,
    );

    match named_type {
        // The fragment applies to the object type if its type is the same object type
        Some(s::TypeDefinition::Object(ot)) => object_type == ot,

        // The fragment also applies to the object type if its type is an interface
        // that the object type implements
        Some(s::TypeDefinition::Interface(it)) => object_type
            .implements_interfaces
            .iter()
            .find(|name| name == &&it.name)
            .map(|_| true)
            .unwrap_or(false),

        // The fragment also applies to an object type if its type is a union that
        // the object type is one of the possible types for
        Some(s::TypeDefinition::Union(ut)) => ut
            .types
            .iter()
            .find(|name| name == &&object_type.name)
            .map(|_| true)
            .unwrap_or(false),

        // In all other cases, the fragment does not apply
        _ => false,
    }
}

/// Executes a field.
fn execute_field<'a, R1, R2>(
    ctx: ExecutionContext<'a, R1, R2>,
    object_type: &s::ObjectType,
    object_value: &Option<q::Value>,
    field: &'a q::Field,
    field_definition: &s::Field,
    fields: Vec<&'a q::Field>,
) -> Result<q::Value, QueryExecutionError>
where
    R1: Resolver,
    R2: Resolver,
{
    coerce_argument_values(ctx.clone(), object_type, field)
        .and_then(|argument_values| {
            resolve_field_value(
                ctx.clone(),
                object_type,
                object_value,
                field,
                field_definition,
                &field_definition.field_type,
                &argument_values,
            )
        })
        .and_then(|value| complete_value(ctx, field, &field_definition.field_type, fields, value))
}

/// Resolves the value of a field.
fn resolve_field_value<'a, R1, R2>(
    ctx: ExecutionContext<'a, R1, R2>,
    object_type: &s::ObjectType,
    object_value: &Option<q::Value>,
    field: &q::Field,
    field_definition: &s::Field,
    field_type: &s::Type,
    argument_values: &HashMap<&q::Name, q::Value>,
) -> Result<q::Value, QueryExecutionError>
where
    R1: Resolver,
    R2: Resolver,
{
    match field_type {
        s::Type::NonNullType(inner_type) => resolve_field_value(
            ctx,
            object_type,
            object_value,
            field,
            field_definition,
            inner_type.as_ref(),
            argument_values,
        ),

        s::Type::NamedType(ref name) => resolve_field_value_for_named_type(
            ctx,
            object_value,
            field,
            field_definition,
            name,
            argument_values,
        ),

        s::Type::ListType(inner_type) => resolve_field_value_for_list_type(
            ctx,
            object_type,
            object_value,
            field,
            field_definition,
            inner_type.as_ref(),
            argument_values,
        ),
    }
}

/// Resolves the value of a field that corresponds to a named type.
fn resolve_field_value_for_named_type<'a, R1, R2>(
    ctx: ExecutionContext<'a, R1, R2>,
    object_value: &Option<q::Value>,
    field: &q::Field,
    field_definition: &s::Field,
    type_name: &s::Name,
    argument_values: &HashMap<&q::Name, q::Value>,
) -> Result<q::Value, QueryExecutionError>
where
    R1: Resolver,
    R2: Resolver,
{
    // Try to resolve the type name into the actual type
    let named_type = sast::get_named_type(
        if ctx.introspecting {
            ctx.introspection_schema
        } else {
            &ctx.schema.document
        },
        type_name,
    ).ok_or(QueryExecutionError::NamedTypeError(type_name.to_string()))?;

    match named_type {
        // Let the resolver decide how the field (with the given object type)
        // is resolved into an entity based on the (potential) parent object
        s::TypeDefinition::Object(t) => if ctx.introspecting {
            Ok(ctx.introspection_resolver.resolve_object(
                object_value,
                &field.name,
                field_definition,
                t,
                argument_values,
            ))
        } else {
            Ok(ctx.resolver.resolve_object(
                object_value,
                &field.name,
                field_definition,
                t,
                argument_values,
            ))
        },

        // Let the resolver decide how values in the resolved object value
        // map to values of GraphQL enums
        s::TypeDefinition::Enum(t) => match object_value {
            Some(q::Value::Object(o)) => if ctx.introspecting {
                Ok(ctx
                    .introspection_resolver
                    .resolve_enum_value(t, o.get(&field.name)))
            } else {
                Ok(ctx.resolver.resolve_enum_value(t, o.get(&field.name)))
            },
            _ => Ok(q::Value::Null),
        },

        // Let the resolver decide how values in the resolved object value
        // map to values of GraphQL scalars
        s::TypeDefinition::Scalar(t) => match object_value {
            Some(q::Value::Object(o)) => if ctx.introspecting {
                Ok(ctx
                    .introspection_resolver
                    .resolve_scalar_value(t, o.get(&field.name)))
            } else {
                Ok(ctx.resolver.resolve_scalar_value(t, o.get(&field.name)))
            },
            _ => Ok(q::Value::Null),
        },

        // We will implement these later
        s::TypeDefinition::Interface(_) => unimplemented!(),
        s::TypeDefinition::Union(_) => unimplemented!(),

        _ => unimplemented!(),
    }
}

/// Resolves the value of a field that corresponds to a list type.
fn resolve_field_value_for_list_type<'a, R1, R2>(
    ctx: ExecutionContext<'a, R1, R2>,
    object_type: &s::ObjectType,
    object_value: &Option<q::Value>,
    field: &q::Field,
    field_definition: &s::Field,
    inner_type: &s::Type,
    argument_values: &HashMap<&q::Name, q::Value>,
) -> Result<q::Value, QueryExecutionError>
where
    R1: Resolver,
    R2: Resolver,
{
    match inner_type {
        s::Type::NonNullType(inner_type) => resolve_field_value_for_list_type(
            ctx,
            object_type,
            object_value,
            field,
            field_definition,
            inner_type,
            argument_values,
        ),

        s::Type::NamedType(ref type_name) => {
            let named_type = sast::get_named_type(
                if ctx.introspecting {
                    ctx.introspection_schema
                } else {
                    &ctx.schema.document
                },
                type_name,
            ).expect("Failed to resolve named type inside list type");

            match named_type {
                // Let the resolver decide how the list field (with the given item object type)
                // is resolved into a entities based on the (potential) parent object
                s::TypeDefinition::Object(t) => if ctx.introspecting {
                    Ok(ctx.introspection_resolver.resolve_objects(
                        object_value,
                        &field.name,
                        field_definition,
                        t,
                        argument_values,
                    ))
                } else {
                    Ok(ctx.resolver.resolve_objects(
                        object_value,
                        &field.name,
                        field_definition,
                        t,
                        argument_values,
                    ))
                },

                // Let the resolver decide how values in the resolved object value
                // map to values of GraphQL enums
                s::TypeDefinition::Enum(t) => match object_value {
                    Some(q::Value::Object(o)) => if ctx.introspecting {
                        Ok(ctx
                            .introspection_resolver
                            .resolve_enum_values(t, o.get(&field.name)))
                    } else {
                        Ok(ctx.resolver.resolve_enum_values(t, o.get(&field.name)))
                    },
                    _ => Ok(q::Value::Null),
                },

                // Let the resolver decide how values in the resolved object value
                // map to values of GraphQL scalars
                s::TypeDefinition::Scalar(t) => match object_value {
                    Some(q::Value::Object(o)) => if ctx.introspecting {
                        Ok(ctx
                            .introspection_resolver
                            .resolve_scalar_values(t, o.get(&field.name)))
                    } else {
                        Ok(ctx.resolver.resolve_scalar_values(t, o.get(&field.name)))
                    },
                    _ => Ok(q::Value::Null),
                },

                // We will implement these later
                s::TypeDefinition::Interface(_) => unimplemented!(),
                s::TypeDefinition::Union(_) => unimplemented!(),

                _ => unimplemented!(),
            }
        }

        // We don't support nested lists yet
        s::Type::ListType(_) => unimplemented!(),
    }
}

/// Ensures that a value matches the expected return type.
fn complete_value<'a, R1, R2>(
    ctx: ExecutionContext<'a, R1, R2>,
    field: &'a q::Field,
    field_type: &'a s::Type,
    fields: Vec<&'a q::Field>,
    resolved_value: q::Value,
) -> Result<q::Value, QueryExecutionError>
where
    R1: Resolver,
    R2: Resolver,
{
    // Fail if the field type is non-null but the value is null
    if let s::Type::NonNullType(inner_type) = field_type {
        return match complete_value(ctx.clone(), field, inner_type, fields, resolved_value)? {
            q::Value::Null => Err(QueryExecutionError::NonNullError(
                field.position,
                field.name.to_string(),
            )),

            v => Ok(v),
        };
    }

    // If the resolved value is null, return null
    if resolved_value == q::Value::Null {
        return Ok(resolved_value);
    }

    // Complete list values
    if let s::Type::ListType(inner_type) = field_type {
        return match resolved_value {
            // Complete list values individually
            q::Value::List(values) => {
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
                Ok(q::Value::List(out))
            }

            // Return field error if the resolved value for the list is not a list
            _ => Err(QueryExecutionError::ListValueError(
                field.position,
                field.name.to_string(),
            )),
        };
    }

    let named_type = if let s::Type::NamedType(name) = field_type {
        Some(
            sast::get_named_type(
                if ctx.introspecting {
                    ctx.introspection_schema
                } else {
                    &ctx.schema.document
                },
                name,
            ).unwrap(),
        )
    } else {
        None
    };

    match named_type {
        // Complete scalar values; we're assuming that the resolver has
        // already returned a valid value for the scalar type
        Some(s::TypeDefinition::Scalar(_)) => return Ok(resolved_value),

        // Complete enum values; we're assuming that the resolver has
        // already returned a valid value for the enum type
        Some(s::TypeDefinition::Enum(_)) => return Ok(resolved_value),

        // Complete object types recursively
        Some(s::TypeDefinition::Object(object_type)) => execute_selection_set(
            ctx.clone(),
            &merge_selection_sets(fields),
            object_type,
            &Some(resolved_value),
        ).map(|result| match result.data {
            Some(v) => v,
            None => q::Value::Null,
        }),

        // Resolve interface types using the resolved value and complete the value recursively
        Some(s::TypeDefinition::Interface(_)) => {
            let object_type =
                resolve_abstract_type(ctx.clone(), named_type.unwrap(), &resolved_value)?;

            execute_selection_set(
                ctx.clone(),
                &merge_selection_sets(fields),
                object_type,
                &Some(resolved_value),
            ).map(|result| match result.data {
                Some(v) => v,
                None => q::Value::Null,
            })
        }

        // Resolve union types using the resolved value and complete the value recursively
        Some(s::TypeDefinition::Union(_)) => {
            let object_type =
                resolve_abstract_type(ctx.clone(), named_type.unwrap(), &resolved_value)?;

            execute_selection_set(
                ctx.clone(),
                &merge_selection_sets(fields),
                object_type,
                &Some(resolved_value),
            ).map(|result| match result.data {
                Some(v) => v,
                None => q::Value::Null,
            })
        }

        _ => unimplemented!(),
    }
}

/// Resolves an abstract type (interface, union) into an object type based on the given value.
fn resolve_abstract_type<'a, R1, R2>(
    ctx: ExecutionContext<'a, R1, R2>,
    abstract_type: &'a s::TypeDefinition,
    object_value: &q::Value,
) -> Result<&'a s::ObjectType, QueryExecutionError>
where
    R1: Resolver,
    R2: Resolver,
{
    // Let the resolver handle the type resolution, return an error if the resolution
    // yields nothing
    ctx.resolver
        .resolve_abstract_type(
            if ctx.introspecting {
                ctx.introspection_schema
            } else {
                &ctx.schema.document
            },
            abstract_type,
            object_value,
        )
        .ok_or(QueryExecutionError::AbstractTypeError(
            sast::get_type_name(abstract_type).to_string(),
        ))
}

/// Merges the selection sets of several fields into a single selection set.
fn merge_selection_sets(fields: Vec<&q::Field>) -> q::SelectionSet {
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

    q::SelectionSet {
        span: span.unwrap(),
        items,
    }
}

/// Coerces argument values into GraphQL values.
pub fn coerce_argument_values<'a, R1, R2>(
    ctx: ExecutionContext<'a, R1, R2>,
    object_type: &'a s::ObjectType,
    field: &'a q::Field,
) -> Result<HashMap<&'a q::Name, q::Value>, QueryExecutionError>
where
    R1: Resolver,
    R2: Resolver,
{
    let mut coerced_values = HashMap::new();

    if let Some(argument_definitions) = sast::get_argument_definitions(object_type, &field.name) {
        for argument_def in argument_definitions.iter() {
            match qast::get_argument_value(&field.arguments, &argument_def.name) {
                // We don't support variables yet
                Some(q::Value::Variable(_)) => unimplemented!(),

                // There is no value, either use the default or fail
                None => {
                    if let Some(ref default_value) = argument_def.default_value {
                        coerced_values.insert(&argument_def.name, default_value.clone());
                    } else if let s::Type::NonNullType(_) = argument_def.value_type {
                        return Err(QueryExecutionError::MissingArgumentError(
                            field.position.clone(),
                            argument_def.name.to_owned(),
                        ));
                    };
                }

                // There is a value for the argument, attempt to coerce it to the
                // value type of the argument definition
                Some(v) => {
                    coerced_values.insert(
                        &argument_def.name,
                        coerce_argument_value(ctx.clone(), field, argument_def, v)?,
                    );
                }
            };
        }
    };

    Ok(coerced_values)
}

/// Coerces a single argument value into a GraphQL value.
fn coerce_argument_value<'a, R1, R2>(
    ctx: ExecutionContext<'a, R1, R2>,
    field: &q::Field,
    argument: &s::InputValue,
    value: &q::Value,
) -> Result<q::Value, QueryExecutionError>
where
    R1: Resolver,
    R2: Resolver,
{
    use graphql_parser::schema::Name;
    use values::coercion::coerce_value;

    let resolver = |name: &Name| {
        sast::get_named_type(
            if ctx.introspecting {
                ctx.introspection_schema
            } else {
                &ctx.schema.document
            },
            name,
        )
    };

    coerce_value(&value, &argument.value_type, &resolver).ok_or_else(|| {
        QueryExecutionError::InvalidArgumentError(
            field.position.clone(),
            argument.name.to_owned(),
            value.clone(),
        )
    })
}

fn get_field_type<'a, R1, R2>(
    ctx: ExecutionContext<'a, R1, R2>,
    object_type: &'a s::ObjectType,
    name: &'a s::Name,
) -> Option<(&'a s::Field, bool)>
where
    R1: Resolver,
    R2: Resolver,
{
    // Resolve __schema and __Type using the introspection schema
    if Some(object_type) == sast::get_root_query_type(&ctx.schema.document) {
        if let Some(ref object_type) = sast::get_root_query_type(ctx.introspection_schema) {
            match sast::get_field_type(object_type, name).map(|t| (t, true)) {
                Some(v) => return Some(v),
                None => (),
            }
        }
    }

    sast::get_field_type(object_type, name).map(|t| (t, ctx.introspecting))
}
