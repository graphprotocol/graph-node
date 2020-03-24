use graphql_parser::query as q;
use graphql_parser::schema as s;
use indexmap::IndexMap;
use lazy_static::lazy_static;
use std::cmp;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::Deref;
use std::time::Instant;

use graph::data::graphql::ext::TypeExt;
use graph::prelude::*;

use crate::introspection::INTROSPECTION_DOCUMENT;
use crate::prelude::*;
use crate::query::ast as qast;
use crate::query::ext::FieldExt as _;
use crate::schema::ast as sast;
use crate::values::coercion;

lazy_static! {
    static ref NO_PREFETCH: bool = std::env::var_os("GRAPH_GRAPHQL_NO_PREFETCH").is_some();
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ExecutionMode {
    Prefetch,
    Verify,
}

/// Contextual information passed around during query execution.
#[derive(Clone)]
pub struct ExecutionContext<R>
where
    R: Resolver,
{
    /// The logger to use.
    pub logger: Logger,

    /// The schema to execute the query against.
    pub schema: Arc<Schema>,

    /// The query to execute.
    pub document: q::Document,

    /// The resolver to use.
    pub resolver: Arc<R>,

    /// The current field stack (e.g. allUsers > friends > name).
    pub fields: Vec<q::Field>,

    /// Variable values.
    pub variable_values: Arc<HashMap<q::Name, q::Value>>,

    /// Time at which the query times out.
    pub deadline: Option<Instant>,

    /// Max value for `first`.
    pub max_first: u32,

    /// The block at which we should execute the query. Initialize this
    /// with `BLOCK_NUMBER_MAX` to get the latest data
    pub block: BlockNumber,

    pub mode: ExecutionMode,
}

#[derive(Copy, Clone, Debug)]
pub(crate) enum ComplexityError {
    TooDeep,
    Overflow,
    Invalid,
}

// Helpers to look for types and fields on both the introspection and regular schemas.
fn get_named_type(schema: &s::Document, name: &Name) -> Option<s::TypeDefinition> {
    if name.starts_with("__") {
        sast::get_named_type(&INTROSPECTION_DOCUMENT, name).cloned()
    } else {
        sast::get_named_type(schema, name).cloned()
    }
}

fn get_field<'a>(object_type: impl Into<ObjectOrInterface<'a>>, name: &Name) -> Option<s::Field> {
    if name == "__schema" || name == "__type" {
        let object_type = sast::get_root_query_type(&INTROSPECTION_DOCUMENT).unwrap();
        sast::get_field(object_type, name).cloned()
    } else {
        sast::get_field(object_type, name).cloned()
    }
}

impl<R> ExecutionContext<R>
where
    R: Resolver,
{
    /// Creates a derived context for a new field (added to the top of the field stack).
    pub fn for_field<'a>(
        &self,
        field: &q::Field,
        object_type: impl Into<ObjectOrInterface<'a>>,
    ) -> Result<Self, QueryExecutionError> {
        let mut ctx = self.clone();
        ctx.fields.push(field.clone());
        if let Some(bc) = field.block_constraint(object_type)? {
            ctx.block = self.resolver.locate_block(&bc)?;
        }
        Ok(ctx)
    }

    pub fn as_introspection_context(&self) -> ExecutionContext<IntrospectionResolver> {
        // Create an introspection type store and resolver
        let introspection_schema = introspection_schema(self.schema.id.clone());
        let introspection_resolver = IntrospectionResolver::new(&self.logger, &self.schema);

        ExecutionContext {
            logger: self.logger.clone(),
            resolver: Arc::new(introspection_resolver),
            schema: Arc::new(introspection_schema),
            document: self.document.clone(),
            fields: vec![],
            variable_values: self.variable_values.clone(),
            deadline: self.deadline,
            max_first: std::u32::MAX,
            block: self.block,
            mode: ExecutionMode::Prefetch,
        }
    }

    /// See https://developer.github.com/v4/guides/resource-limitations/.
    ///
    /// If the query is invalid, returns `Ok(0)` so that execution proceeds and
    /// gives a proper error.
    pub(crate) fn root_query_complexity(
        &self,
        root_type: &s::TypeDefinition,
        root_selection_set: &q::SelectionSet,
        max_depth: u8,
    ) -> Result<u64, QueryExecutionError> {
        match self.query_complexity(root_type, root_selection_set, max_depth, 0) {
            Ok(complexity) => Ok(complexity),
            Err(ComplexityError::Invalid) => Ok(0),
            Err(ComplexityError::TooDeep) => Err(QueryExecutionError::TooDeep(max_depth)),
            Err(ComplexityError::Overflow) => {
                Err(QueryExecutionError::TooComplex(u64::max_value(), 0))
            }
        }
    }

    fn query_complexity(
        &self,
        ty: &s::TypeDefinition,
        selection_set: &q::SelectionSet,
        max_depth: u8,
        depth: u8,
    ) -> Result<u64, ComplexityError> {
        use ComplexityError::*;

        if depth >= max_depth {
            return Err(TooDeep);
        }

        selection_set
            .items
            .iter()
            .try_fold(0, |total_complexity, selection| {
                let schema = &self.schema.document;
                match selection {
                    q::Selection::Field(field) => {
                        // Empty selection sets are the base case.
                        if field.selection_set.items.is_empty() {
                            return Ok(total_complexity);
                        }

                        // Get field type to determine if this is a collection query.
                        let s_field = match ty {
                            s::TypeDefinition::Object(t) => get_field(t, &field.name),
                            s::TypeDefinition::Interface(t) => get_field(t, &field.name),

                            // `Scalar` and `Enum` cannot have selection sets.
                            // `InputObject` can't appear in a selection.
                            // `Union` is not yet supported.
                            s::TypeDefinition::Scalar(_)
                            | s::TypeDefinition::Enum(_)
                            | s::TypeDefinition::InputObject(_)
                            | s::TypeDefinition::Union(_) => None,
                        }
                        .ok_or(Invalid)?;

                        let field_complexity = self.query_complexity(
                            &get_named_type(schema, s_field.field_type.get_base_type())
                                .ok_or(Invalid)?,
                            &field.selection_set,
                            max_depth,
                            depth + 1,
                        )?;

                        // Non-collection queries pass through.
                        if !sast::is_list_or_non_null_list_field(&s_field) {
                            return Ok(total_complexity + field_complexity);
                        }

                        // For collection queries, check the `first` argument.
                        let max_entities = qast::get_argument_value(&field.arguments, "first")
                            .and_then(|arg| match arg {
                                q::Value::Int(n) => Some(n.as_i64()? as u64),
                                _ => None,
                            })
                            .unwrap_or(100);
                        max_entities
                            .checked_add(
                                max_entities.checked_mul(field_complexity).ok_or(Overflow)?,
                            )
                            .ok_or(Overflow)
                    }
                    q::Selection::FragmentSpread(fragment) => {
                        let def = qast::get_fragment(&self.document, &fragment.fragment_name)
                            .ok_or(Invalid)?;
                        let q::TypeCondition::On(type_name) = &def.type_condition;
                        let ty = get_named_type(schema, &type_name).ok_or(Invalid)?;
                        self.query_complexity(&ty, &def.selection_set, max_depth, depth + 1)
                    }
                    q::Selection::InlineFragment(fragment) => {
                        let ty = match &fragment.type_condition {
                            Some(q::TypeCondition::On(type_name)) => {
                                get_named_type(schema, &type_name).ok_or(Invalid)?
                            }
                            _ => ty.clone(),
                        };
                        self.query_complexity(&ty, &fragment.selection_set, max_depth, depth + 1)
                    }
                }
                .and_then(|complexity| total_complexity.checked_add(complexity).ok_or(Overflow))
            })
    }

    // Checks for invalid selections.
    pub(crate) fn validate_fields(
        &self,
        type_name: &Name,
        ty: &s::TypeDefinition,
        selection_set: &q::SelectionSet,
    ) -> Vec<QueryExecutionError> {
        let schema = &self.schema.document;
        selection_set
            .items
            .iter()
            .fold(vec![], |mut errors, selection| {
                match selection {
                    q::Selection::Field(field) => {
                        // Get field type to determine if this is a collection query.
                        let s_field = match ty {
                            s::TypeDefinition::Object(t) => get_field(t, &field.name),
                            s::TypeDefinition::Interface(t) => get_field(t, &field.name),

                            // `Scalar` and `Enum` cannot have selection sets.
                            // `InputObject` can't appear in a selection.
                            // `Union` is not yet supported.
                            s::TypeDefinition::Scalar(_)
                            | s::TypeDefinition::Enum(_)
                            | s::TypeDefinition::InputObject(_)
                            | s::TypeDefinition::Union(_) => None,
                        };

                        match s_field {
                            Some(s_field) => {
                                let base_type = s_field.field_type.get_base_type();
                                match get_named_type(schema, base_type) {
                                    Some(ty) => errors.extend(self.validate_fields(
                                        base_type,
                                        &ty,
                                        &field.selection_set,
                                    )),
                                    None => errors.push(QueryExecutionError::NamedTypeError(
                                        base_type.clone(),
                                    )),
                                }
                            }
                            None => errors.push(QueryExecutionError::UnknownField(
                                field.position,
                                type_name.clone(),
                                field.name.clone(),
                            )),
                        }
                    }
                    q::Selection::FragmentSpread(fragment) => {
                        match qast::get_fragment(&self.document, &fragment.fragment_name) {
                            Some(frag) => {
                                let q::TypeCondition::On(type_name) = &frag.type_condition;
                                match get_named_type(schema, type_name) {
                                    Some(ty) => errors.extend(self.validate_fields(
                                        type_name,
                                        &ty,
                                        &frag.selection_set,
                                    )),
                                    None => errors.push(QueryExecutionError::NamedTypeError(
                                        type_name.clone(),
                                    )),
                                }
                            }
                            None => errors.push(QueryExecutionError::UndefinedFragment(
                                fragment.fragment_name.clone(),
                            )),
                        }
                    }
                    q::Selection::InlineFragment(fragment) => match &fragment.type_condition {
                        Some(q::TypeCondition::On(type_name)) => {
                            match get_named_type(schema, type_name) {
                                Some(ty) => errors.extend(self.validate_fields(
                                    type_name,
                                    &ty,
                                    &fragment.selection_set,
                                )),
                                None => errors
                                    .push(QueryExecutionError::NamedTypeError(type_name.clone())),
                            }
                        }
                        _ => errors.extend(self.validate_fields(
                            type_name,
                            ty,
                            &fragment.selection_set,
                        )),
                    },
                }
                errors
            })
    }
}

/// Executes the root selection set of a query.
pub fn execute_root_selection_set(
    ctx: &ExecutionContext<impl Resolver>,
    selection_set: &q::SelectionSet,
) -> Result<q::Value, Vec<QueryExecutionError>> {
    // Obtain the root Query type and fail if there isn't one
    let query_type = match sast::get_root_query_type(&ctx.schema.document) {
        Some(t) => t,
        None => return Err(vec![QueryExecutionError::NoRootQueryObjectType]),
    };

    // Split the toplevel fields into introspection fields and
    // regular data fields
    let mut data_set = q::SelectionSet {
        span: selection_set.span.clone(),
        items: Vec::new(),
    };
    let mut intro_set = q::SelectionSet {
        span: selection_set.span.clone(),
        items: Vec::new(),
    };

    let ictx = ctx.as_introspection_context();
    let introspection_query_type = sast::get_root_query_type(&ictx.schema.document).unwrap();
    for (_, fields) in collect_fields(ctx, query_type, selection_set, None) {
        let name = fields[0].name.clone();
        let selections = fields.into_iter().map(|f| q::Selection::Field(f.clone()));
        // See if this is an introspection or data field. We don't worry about
        // nonexistant fields; those will cause an error later when we execute
        // the data_set SelectionSet
        if sast::get_field(introspection_query_type, &name).is_some() {
            intro_set.items.extend(selections)
        } else {
            data_set.items.extend(selections)
        }
    }

    // If we are getting regular data, prefetch it from the database
    let mut values = if data_set.items.is_empty() {
        BTreeMap::default()
    } else {
        // Allow turning prefetch off as a safety valve. Once we are confident
        // prefetching contains no more bugs, we should remove this env variable
        let initial_data = if *NO_PREFETCH {
            None
        } else {
            ctx.resolver.prefetch(&ctx, &data_set)?
        };
        let values = execute_selection_set_to_map(&ctx, &data_set, query_type, &initial_data)?;
        if ctx.mode == ExecutionMode::Verify {
            let single_values = execute_selection_set_to_map(&ctx, &data_set, query_type, &None)?;
            if values != single_values {
                return Err(vec![QueryExecutionError::IncorrectPrefetchResult {
                    slow: q::Value::Object(single_values),
                    prefetch: q::Value::Object(values),
                }]);
            }
        }
        values
    };

    // Resolve introspection fields, if there are any
    if !intro_set.items.is_empty() {
        values.extend(execute_selection_set_to_map(
            &ictx,
            &intro_set,
            introspection_query_type,
            &None,
        )?);
    }
    Ok(q::Value::Object(values))
}

/// Executes a selection set, requiring the result to be of the given object type.
///
/// Allows passing in a parent value during recursive processing of objects and their fields.
pub fn execute_selection_set(
    ctx: &ExecutionContext<impl Resolver>,
    selection_set: &q::SelectionSet,
    object_type: &s::ObjectType,
    object_value: &Option<q::Value>,
) -> Result<q::Value, Vec<QueryExecutionError>> {
    Ok(q::Value::Object(execute_selection_set_to_map(
        ctx,
        selection_set,
        object_type,
        object_value,
    )?))
}

fn execute_selection_set_to_map(
    ctx: &ExecutionContext<impl Resolver>,
    selection_set: &q::SelectionSet,
    object_type: &s::ObjectType,
    object_value: &Option<q::Value>,
) -> Result<BTreeMap<String, q::Value>, Vec<QueryExecutionError>> {
    let mut errors: Vec<QueryExecutionError> = Vec::new();
    let mut result_map: BTreeMap<String, q::Value> = BTreeMap::new();

    // Group fields with the same response key, so we can execute them together
    let grouped_field_set = collect_fields(ctx, object_type, selection_set, None);

    // Process all field groups in order
    for (response_key, fields) in grouped_field_set {
        match ctx.deadline {
            Some(deadline) if deadline < Instant::now() => {
                errors.push(QueryExecutionError::Timeout);
                break;
            }
            _ => (),
        }

        // If the field exists on the object, execute it and add its result to the result map
        if let Some(ref field) = sast::get_field(object_type, &fields[0].name) {
            // Push the new field onto the context's field stack
            match ctx.for_field(&fields[0], object_type) {
                Ok(ctx) => {
                    match execute_field(&ctx, object_type, object_value, &fields[0], field, fields)
                    {
                        Ok(v) => {
                            result_map.insert(response_key.to_owned(), v);
                        }
                        Err(mut e) => {
                            errors.append(&mut e);
                        }
                    };
                }
                Err(e) => errors.push(e),
            }
        } else {
            errors.push(QueryExecutionError::UnknownField(
                fields[0].position,
                object_type.name.clone(),
                fields[0].name.clone(),
            ))
        }
    }

    if errors.is_empty() && !result_map.is_empty() {
        Ok(result_map)
    } else {
        if errors.is_empty() {
            errors.push(QueryExecutionError::EmptySelectionSet(
                object_type.name.clone(),
            ));
        }
        Err(errors)
    }
}

/// Collects fields of a selection set.
pub fn collect_fields<'a>(
    ctx: &'a ExecutionContext<impl Resolver>,
    object_type: &s::ObjectType,
    selection_set: &'a q::SelectionSet,
    visited_fragments: Option<HashSet<&'a q::Name>>,
) -> IndexMap<&'a String, Vec<&'a q::Field>> {
    let mut visited_fragments = visited_fragments.unwrap_or_default();
    let mut grouped_fields: IndexMap<_, Vec<_>> = IndexMap::new();

    // Only consider selections that are not skipped and should be included
    let selections: Vec<_> = selection_set
        .items
        .iter()
        .filter(|selection| !qast::skip_selection(selection, ctx.variable_values.deref()))
        .filter(|selection| qast::include_selection(selection, ctx.variable_values.deref()))
        .collect();

    for selection in selections {
        match selection {
            q::Selection::Field(ref field) => {
                // Obtain the response key for the field
                let response_key = qast::get_response_key(field);

                // Create a field group for this response key on demand and
                // append the selection field to this group.
                grouped_fields.entry(response_key).or_default().push(field);
            }

            q::Selection::FragmentSpread(spread) => {
                // Only consider the fragment if it hasn't already been included,
                // as would be the case if the same fragment spread ...Foo appeared
                // twice in the same selection set
                if !visited_fragments.contains(&spread.fragment_name) {
                    visited_fragments.insert(&spread.fragment_name);

                    // Resolve the fragment using its name and, if it applies, collect
                    // fields for the fragment and group them
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
                            let fragment_grouped_field_set = collect_fields(
                                ctx,
                                object_type,
                                &fragment.selection_set,
                                Some(visited_fragments.clone()),
                            );

                            // Add all items from each fragments group to the field group
                            // with the corresponding response key
                            for (response_key, mut fragment_group) in fragment_grouped_field_set {
                                grouped_fields
                                    .entry(response_key)
                                    .or_default()
                                    .append(&mut fragment_group);
                            }
                        });
                }
            }

            q::Selection::InlineFragment(fragment) => {
                let applies = match &fragment.type_condition {
                    Some(cond) => does_fragment_type_apply(ctx.clone(), object_type, &cond),
                    None => true,
                };

                if applies {
                    let fragment_grouped_field_set = collect_fields(
                        ctx,
                        object_type,
                        &fragment.selection_set,
                        Some(visited_fragments.clone()),
                    );

                    for (response_key, mut fragment_group) in fragment_grouped_field_set {
                        grouped_fields
                            .entry(response_key)
                            .or_default()
                            .append(&mut fragment_group);
                    }
                }
            }
        };
    }

    grouped_fields
}

/// Determines whether a fragment is applicable to the given object type.
fn does_fragment_type_apply(
    ctx: ExecutionContext<impl Resolver>,
    object_type: &s::ObjectType,
    fragment_type: &q::TypeCondition,
) -> bool {
    // This is safe to do, as TypeCondition only has a single `On` variant.
    let q::TypeCondition::On(ref name) = fragment_type;

    // Resolve the type the fragment applies to based on its name
    let named_type = sast::get_named_type(&ctx.schema.document, name);

    match named_type {
        // The fragment applies to the object type if its type is the same object type
        Some(s::TypeDefinition::Object(ot)) => object_type == ot,

        // The fragment also applies to the object type if its type is an interface
        // that the object type implements
        Some(s::TypeDefinition::Interface(it)) => {
            object_type.implements_interfaces.contains(&it.name)
        }

        // The fragment also applies to an object type if its type is a union that
        // the object type is one of the possible types for
        Some(s::TypeDefinition::Union(ut)) => ut.types.contains(&object_type.name),

        // In all other cases, the fragment does not apply
        _ => false,
    }
}

/// Executes a field.
fn execute_field(
    ctx: &ExecutionContext<impl Resolver>,
    object_type: &s::ObjectType,
    object_value: &Option<q::Value>,
    field: &q::Field,
    field_definition: &s::Field,
    fields: Vec<&q::Field>,
) -> Result<q::Value, Vec<QueryExecutionError>> {
    coerce_argument_values(ctx, object_type, field)
        .and_then(|argument_values| {
            resolve_field_value(
                ctx,
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
fn resolve_field_value(
    ctx: &ExecutionContext<impl Resolver>,
    object_type: &s::ObjectType,
    object_value: &Option<q::Value>,
    field: &q::Field,
    field_definition: &s::Field,
    field_type: &s::Type,
    argument_values: &HashMap<&q::Name, q::Value>,
) -> Result<q::Value, Vec<QueryExecutionError>> {
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
            object_type,
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
fn resolve_field_value_for_named_type(
    ctx: &ExecutionContext<impl Resolver>,
    object_type: &s::ObjectType,
    object_value: &Option<q::Value>,
    field: &q::Field,
    field_definition: &s::Field,
    type_name: &s::Name,
    argument_values: &HashMap<&q::Name, q::Value>,
) -> Result<q::Value, Vec<QueryExecutionError>> {
    // Try to resolve the type name into the actual type
    let named_type = sast::get_named_type(&ctx.schema.document, type_name)
        .ok_or_else(|| QueryExecutionError::NamedTypeError(type_name.to_string()))?;
    match named_type {
        // Let the resolver decide how the field (with the given object type)
        // is resolved into an entity based on the (potential) parent object
        s::TypeDefinition::Object(t) => ctx.resolver.resolve_object(
            object_value,
            field,
            field_definition,
            t.into(),
            argument_values,
            ctx.schema.types_for_interface(),
            ctx.block,
        ),

        // Let the resolver decide how values in the resolved object value
        // map to values of GraphQL enums
        s::TypeDefinition::Enum(t) => match object_value {
            Some(q::Value::Object(o)) => {
                ctx.resolver
                    .resolve_enum_value(field, t, o.get(&field.name))
            }
            _ => Ok(q::Value::Null),
        },

        // Let the resolver decide how values in the resolved object value
        // map to values of GraphQL scalars
        s::TypeDefinition::Scalar(t) => match object_value {
            Some(q::Value::Object(o)) => ctx.resolver.resolve_scalar_value(
                object_type,
                o,
                field,
                t,
                o.get(&field.name),
                argument_values,
            ),
            None => ctx.resolver.resolve_scalar_value(
                object_type,
                &BTreeMap::new(),
                field,
                t,
                None,
                argument_values,
            ),
            _ => Ok(q::Value::Null),
        },

        s::TypeDefinition::Interface(i) => ctx.resolver.resolve_object(
            object_value,
            field,
            field_definition,
            i.into(),
            argument_values,
            ctx.schema.types_for_interface(),
            ctx.block,
        ),

        s::TypeDefinition::Union(_) => Err(QueryExecutionError::Unimplemented("unions".to_owned())),

        s::TypeDefinition::InputObject(_) => unreachable!("input objects are never resolved"),
    }
    .map_err(|e| vec![e])
}

/// Resolves the value of a field that corresponds to a list type.
fn resolve_field_value_for_list_type(
    ctx: &ExecutionContext<impl Resolver>,
    object_type: &s::ObjectType,
    object_value: &Option<q::Value>,
    field: &q::Field,
    field_definition: &s::Field,
    inner_type: &s::Type,
    argument_values: &HashMap<&q::Name, q::Value>,
) -> Result<q::Value, Vec<QueryExecutionError>> {
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
            let named_type = sast::get_named_type(&ctx.schema.document, type_name)
                .ok_or_else(|| QueryExecutionError::NamedTypeError(type_name.to_string()))?;

            match named_type {
                // Let the resolver decide how the list field (with the given item object type)
                // is resolved into a entities based on the (potential) parent object
                s::TypeDefinition::Object(t) => ctx
                    .resolver
                    .resolve_objects(
                        object_value,
                        field,
                        field_definition,
                        t.into(),
                        argument_values,
                        ctx.schema.types_for_interface(),
                        ctx.block,
                        ctx.max_first,
                    )
                    .map_err(|e| vec![e]),

                // Let the resolver decide how values in the resolved object value
                // map to values of GraphQL enums
                s::TypeDefinition::Enum(t) => match object_value {
                    Some(q::Value::Object(o)) => {
                        ctx.resolver
                            .resolve_enum_values(field, &t, o.get(&field.name))
                    }
                    _ => Ok(q::Value::Null),
                },

                // Let the resolver decide how values in the resolved object value
                // map to values of GraphQL scalars
                s::TypeDefinition::Scalar(t) => match object_value {
                    Some(q::Value::Object(o)) => {
                        ctx.resolver
                            .resolve_scalar_values(field, &t, o.get(&field.name))
                    }
                    _ => Ok(q::Value::Null),
                },

                s::TypeDefinition::Interface(t) => ctx
                    .resolver
                    .resolve_objects(
                        object_value,
                        field,
                        field_definition,
                        t.into(),
                        argument_values,
                        ctx.schema.types_for_interface(),
                        ctx.block,
                        ctx.max_first,
                    )
                    .map_err(|e| vec![e]),

                s::TypeDefinition::Union(_) => Err(vec![QueryExecutionError::Unimplemented(
                    "unions".to_owned(),
                )]),

                s::TypeDefinition::InputObject(_) => {
                    unreachable!("input objects are never resolved")
                }
            }
        }

        // We don't support nested lists yet
        s::Type::ListType(_) => Err(vec![QueryExecutionError::Unimplemented(
            "nested list types".to_owned(),
        )]),
    }
}

/// Ensures that a value matches the expected return type.
fn complete_value(
    ctx: &ExecutionContext<impl Resolver>,
    field: &q::Field,
    field_type: &s::Type,
    fields: Vec<&q::Field>,
    resolved_value: q::Value,
) -> Result<q::Value, Vec<QueryExecutionError>> {
    match field_type {
        // Fail if the field type is non-null but the value is null
        s::Type::NonNullType(inner_type) => {
            return match complete_value(ctx, field, inner_type, fields, resolved_value)? {
                q::Value::Null => Err(vec![QueryExecutionError::NonNullError(
                    field.position,
                    field.name.to_string(),
                )]),

                v => Ok(v),
            };
        }

        // If the resolved value is null, return null
        _ if resolved_value == q::Value::Null => {
            return Ok(resolved_value);
        }

        // Complete list values
        s::Type::ListType(inner_type) => {
            match resolved_value {
                // Complete list values individually
                q::Value::List(values) => {
                    let mut errors = Vec::new();
                    let mut out = Vec::with_capacity(values.len());
                    for value in values.into_iter() {
                        match complete_value(ctx, field, inner_type, fields.clone(), value) {
                            Ok(value) => out.push(value),
                            Err(errs) => errors.extend(errs),
                        }
                    }
                    match errors.is_empty() {
                        true => Ok(q::Value::List(out)),
                        false => Err(errors),
                    }
                }

                // Return field error if the resolved value for the list is not a list
                _ => Err(vec![QueryExecutionError::ListValueError(
                    field.position,
                    field.name.to_string(),
                )]),
            }
        }

        s::Type::NamedType(name) => {
            let named_type = sast::get_named_type(&ctx.schema.document, name).unwrap();

            match named_type {
                // Complete scalar values
                s::TypeDefinition::Scalar(scalar_type) => {
                    resolved_value.coerce(scalar_type).ok_or_else(|| {
                        vec![QueryExecutionError::ScalarCoercionError(
                            field.position.clone(),
                            field.name.to_owned(),
                            resolved_value.clone(),
                            scalar_type.name.to_owned(),
                        )]
                    })
                }

                // Complete enum values
                s::TypeDefinition::Enum(enum_type) => {
                    resolved_value.coerce(enum_type).ok_or_else(|| {
                        vec![QueryExecutionError::EnumCoercionError(
                            field.position.clone(),
                            field.name.to_owned(),
                            resolved_value.clone(),
                            enum_type.name.to_owned(),
                            enum_type
                                .values
                                .iter()
                                .map(|value| value.name.to_owned())
                                .collect(),
                        )]
                    })
                }

                // Complete object types recursively
                s::TypeDefinition::Object(object_type) => execute_selection_set(
                    ctx,
                    &merge_selection_sets(fields),
                    object_type,
                    &Some(resolved_value),
                ),

                // Resolve interface types using the resolved value and complete the value recursively
                s::TypeDefinition::Interface(_) => {
                    let object_type = resolve_abstract_type(ctx, named_type, &resolved_value)?;

                    execute_selection_set(
                        ctx,
                        &merge_selection_sets(fields),
                        object_type,
                        &Some(resolved_value),
                    )
                }

                // Resolve union types using the resolved value and complete the value recursively
                s::TypeDefinition::Union(_) => {
                    let object_type = resolve_abstract_type(ctx, named_type, &resolved_value)?;

                    execute_selection_set(
                        ctx,
                        &merge_selection_sets(fields),
                        object_type,
                        &Some(resolved_value),
                    )
                }

                s::TypeDefinition::InputObject(_) => {
                    unreachable!("input objects are never resolved")
                }
            }
        }
    }
}

/// Resolves an abstract type (interface, union) into an object type based on the given value.
fn resolve_abstract_type<'a>(
    ctx: &'a ExecutionContext<impl Resolver>,
    abstract_type: &s::TypeDefinition,
    object_value: &q::Value,
) -> Result<&'a s::ObjectType, Vec<QueryExecutionError>> {
    // Let the resolver handle the type resolution, return an error if the resolution
    // yields nothing
    ctx.resolver
        .resolve_abstract_type(&ctx.schema.document, abstract_type, object_value)
        .ok_or_else(|| {
            vec![QueryExecutionError::AbstractTypeError(
                sast::get_type_name(abstract_type).to_string(),
            )]
        })
}

/// Merges the selection sets of several fields into a single selection set.
pub fn merge_selection_sets(fields: Vec<&q::Field>) -> q::SelectionSet {
    let (span, items) = fields
        .iter()
        .fold((None, vec![]), |(span, mut items), field| {
            (
                // The overal span is the min/max spans of all merged selection sets
                match span {
                    None => Some(field.selection_set.span),
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
pub fn coerce_argument_values<'a>(
    ctx: &ExecutionContext<impl Resolver>,
    object_type: &'a s::ObjectType,
    field: &q::Field,
) -> Result<HashMap<&'a q::Name, q::Value>, Vec<QueryExecutionError>> {
    let mut coerced_values = HashMap::new();
    let mut errors = vec![];

    let resolver = |name: &Name| sast::get_named_type(&ctx.schema.document, name);

    for argument_def in sast::get_argument_definitions(object_type, &field.name)
        .into_iter()
        .flatten()
    {
        let value = qast::get_argument_value(&field.arguments, &argument_def.name).cloned();
        match coercion::coerce_input_value(value, &argument_def, &resolver, &ctx.variable_values) {
            Ok(Some(value)) => {
                if argument_def.name == "text".to_string() {
                    coerced_values.insert(
                        &argument_def.name,
                        q::Value::Object(BTreeMap::from_iter(vec![(field.name.clone(), value)])),
                    );
                } else {
                    coerced_values.insert(&argument_def.name, value);
                }
            }
            Ok(None) => {}
            Err(e) => errors.push(e),
        }
    }

    if errors.is_empty() {
        Ok(coerced_values)
    } else {
        Err(errors)
    }
}

/// Coerces variable values for an operation.
pub fn coerce_variable_values(
    schema: &Schema,
    operation: &q::OperationDefinition,
    variables: &Option<QueryVariables>,
) -> Result<HashMap<q::Name, q::Value>, Vec<QueryExecutionError>> {
    let mut coerced_values = HashMap::new();
    let mut errors = vec![];

    for variable_def in qast::get_variable_definitions(operation)
        .into_iter()
        .flatten()
    {
        // Skip variable if it has an invalid type
        if !sast::is_input_type(&schema.document, &variable_def.var_type) {
            errors.push(QueryExecutionError::InvalidVariableTypeError(
                variable_def.position,
                variable_def.name.to_owned(),
            ));
            continue;
        }

        let value = variables
            .as_ref()
            .and_then(|vars| vars.get(&variable_def.name));

        let value = match value.or(variable_def.default_value.as_ref()) {
            // No variable value provided and no default for non-null type, fail
            None => {
                if sast::is_non_null_type(&variable_def.var_type) {
                    errors.push(QueryExecutionError::MissingVariableError(
                        variable_def.position,
                        variable_def.name.to_owned(),
                    ));
                };
                continue;
            }
            Some(value) => value,
        };

        // We have a variable value, attempt to coerce it to the value type
        // of the variable definition
        coerced_values.insert(
            variable_def.name.to_owned(),
            coerce_variable_value(schema, variable_def, &value)?,
        );
    }

    if errors.is_empty() {
        Ok(coerced_values)
    } else {
        Err(errors)
    }
}

fn coerce_variable_value(
    schema: &Schema,
    variable_def: &q::VariableDefinition,
    value: &q::Value,
) -> Result<q::Value, Vec<QueryExecutionError>> {
    use crate::values::coercion::coerce_value;

    let resolver = |name: &Name| sast::get_named_type(&schema.document, name);

    coerce_value(&value, &variable_def.var_type, &resolver, &HashMap::new()).ok_or_else(|| {
        vec![QueryExecutionError::InvalidArgumentError(
            variable_def.position,
            variable_def.name.to_owned(),
            value.clone(),
        )]
    })
}
