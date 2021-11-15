use graphql_parser::Pos;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::Instant;
use std::{collections::hash_map::DefaultHasher, convert::TryFrom};

use graph::data::graphql::{
    ext::{DocumentExt, TypeExt},
    ObjectOrInterface,
};
use graph::data::query::{Query as GraphDataQuery, QueryVariables};
use graph::data::schema::ApiSchema;
use graph::prelude::{
    info, o, q, r, s, BlockNumber, CheapClone, Logger, QueryExecutionError, TryFromValue,
};

use crate::execution::ast as a;
use crate::introspection::introspection_schema;
use crate::query::{ast as qast, ext::BlockConstraint};
use crate::schema::ast as sast;
use crate::{
    execution::{get_field, get_named_type, object_or_interface},
    schema::api::ErrorPolicy,
};

#[derive(Clone, Debug)]
pub enum ComplexityError {
    TooDeep,
    Overflow,
    Invalid,
    CyclicalFragment(String),
}

#[derive(Copy, Clone)]
enum Kind {
    Query,
    Subscription,
}

/// Helper to log the fields in a `SelectionSet` without cloning. Writes
/// a list of field names from the selection set separated by ';'. Using
/// ';' as a separator makes parsing the log a little easier since slog
/// uses ',' to separate key/value pairs.
/// If `SelectionSet` is `None`, log `*` to indicate that the query was
/// for the entire selection set of the query
struct SelectedFields<'a>(&'a a::SelectionSet);

impl<'a> std::fmt::Display for SelectedFields<'a> {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        let mut first = true;
        for item in &self.0.items {
            match item {
                a::Selection::Field(field) => {
                    if !first {
                        write!(fmt, ";")?;
                    }
                    first = false;
                    write!(fmt, "{}", field.alias.as_ref().unwrap_or(&field.name))?
                }
                a::Selection::FragmentSpread(_) | a::Selection::InlineFragment(_) => {
                    /* nothing */
                }
            }
        }
        if first {
            // There wasn't a single `q::Selection::Field` in the set. That
            // seems impossible, but log '-' to be on the safe side
            write!(fmt, "-")?;
        }

        Ok(())
    }
}

/// A GraphQL query that has been preprocessed and checked and is ready
/// for execution. Checking includes validating all query fields and, if
/// desired, checking the query's complexity
pub struct Query {
    /// The schema against which to execute the query
    pub schema: Arc<ApiSchema>,
    /// The root selection set of the query. All variable references have already been resolved
    pub selection_set: Arc<a::SelectionSet>,
    /// The ShapeHash of the original query
    pub shape_hash: u64,

    pub network: Option<String>,

    pub logger: Logger,

    start: Instant,

    pub(crate) fragments: HashMap<String, a::FragmentDefinition>,
    kind: Kind,

    /// Used only for logging; if logging is configured off, these will
    /// have dummy values
    pub query_text: Arc<String>,
    pub variables_text: Arc<String>,
    pub query_id: String,
    pub(crate) complexity: u64,
}

impl Query {
    /// Process the raw GraphQL query `query` and prepare for executing it.
    /// The returned `Query` has already been validated and, if `max_complexity`
    /// is given, also checked whether it is too complex. If validation fails,
    /// or the query is too complex, errors are returned
    pub fn new(
        logger: &Logger,
        schema: Arc<ApiSchema>,
        network: Option<String>,
        query: GraphDataQuery,
        max_complexity: Option<u64>,
        max_depth: u8,
    ) -> Result<Arc<Self>, Vec<QueryExecutionError>> {
        let mut operation = None;
        let mut fragments = HashMap::new();
        for defn in query.document.definitions.into_iter() {
            match defn {
                q::Definition::Operation(op) => match operation {
                    None => operation = Some(op),
                    Some(_) => return Err(vec![QueryExecutionError::OperationNameRequired]),
                },
                q::Definition::Fragment(frag) => {
                    fragments.insert(frag.name.clone(), frag);
                }
            }
        }
        let operation = operation.ok_or(QueryExecutionError::OperationNameRequired)?;

        let variables = coerce_variables(schema.as_ref(), &operation, query.variables)?;
        let (kind, selection_set) = match operation {
            q::OperationDefinition::Query(q::Query { selection_set, .. }) => {
                (Kind::Query, selection_set)
            }
            // Queries can be run by just sending a selection set
            q::OperationDefinition::SelectionSet(selection_set) => (Kind::Query, selection_set),
            q::OperationDefinition::Subscription(q::Subscription { selection_set, .. }) => {
                (Kind::Subscription, selection_set)
            }
            q::OperationDefinition::Mutation(_) => {
                return Err(vec![QueryExecutionError::NotSupported(
                    "Mutations are not supported".to_owned(),
                )])
            }
        };

        let query_hash = {
            let mut hasher = DefaultHasher::new();
            query.query_text.hash(&mut hasher);
            query.variables_text.hash(&mut hasher);
            hasher.finish()
        };
        let query_id = format!("{:x}-{:x}", query.shape_hash, query_hash);
        let logger = logger.new(o!(
            "subgraph_id" => schema.id().clone(),
            "query_id" => query_id.clone()
        ));

        let start = Instant::now();
        // Use an intermediate struct so we can modify the query before
        // enclosing it in an Arc
        let raw_query = RawQuery {
            schema: schema.cheap_clone(),
            variables,
            selection_set,
            fragments,
        };

        // It's important to check complexity first, so `validate_fields` doesn't risk a stack
        // overflow from invalid queries.
        let complexity = raw_query.check_complexity(max_complexity, max_depth)?;
        raw_query.validate_fields()?;
        let (selection_set, fragments) = raw_query.expand_variables()?;

        let query = Self {
            schema,
            fragments,
            selection_set: Arc::new(selection_set),
            shape_hash: query.shape_hash,
            kind,
            network,
            logger,
            start,
            query_text: query.query_text.cheap_clone(),
            variables_text: query.variables_text.cheap_clone(),
            query_id,
            complexity,
        };

        Ok(Arc::new(query))
    }

    /// Return the block constraint for the toplevel query field(s), merging the selection sets of
    /// fields that have the same block constraint.
    ///
    /// Also returns the combined error policy for those fields, which is `Deny` if any field is
    /// `Deny` and `Allow` otherwise.
    pub fn block_constraint(
        &self,
    ) -> Result<HashMap<BlockConstraint, (a::SelectionSet, ErrorPolicy)>, Vec<QueryExecutionError>>
    {
        use a::Selection::Field;

        let mut bcs = HashMap::new();
        let mut errors = Vec::new();

        for field in self.selection_set.items.iter().filter_map(|sel| match sel {
            Field(f) => Some(f),
            _ => None,
        }) {
            let query_ty = self.schema.query_type.as_ref();
            let args = match crate::execution::coerce_argument_values(self, query_ty, field) {
                Ok(args) => args,
                Err(errs) => {
                    errors.extend(errs);
                    continue;
                }
            };

            let bc = match args.get("block") {
                Some(bc) => BlockConstraint::try_from_value(bc).map_err(|_| {
                    vec![QueryExecutionError::InvalidArgumentError(
                        Pos::default(),
                        "block".to_string(),
                        bc.clone().into(),
                    )]
                })?,
                None => BlockConstraint::Latest,
            };

            let field_error_policy = match args.get("subgraphError") {
                Some(value) => ErrorPolicy::try_from(value).map_err(|_| {
                    vec![QueryExecutionError::InvalidArgumentError(
                        Pos::default(),
                        "subgraphError".to_string(),
                        value.clone().into(),
                    )]
                })?,
                None => ErrorPolicy::Deny,
            };

            let (selection_set, error_policy) = bcs.entry(bc).or_insert_with(|| {
                (
                    a::SelectionSet::empty_from(&self.selection_set),
                    field_error_policy,
                )
            });
            selection_set.items.push(Field(field.clone()));
            if field_error_policy == ErrorPolicy::Deny {
                *error_policy = ErrorPolicy::Deny;
            }
        }
        if !errors.is_empty() {
            Err(errors)
        } else {
            Ok(bcs)
        }
    }

    /// Return this query, but use the introspection schema as its schema
    pub fn as_introspection_query(&self) -> Arc<Self> {
        let introspection_schema = introspection_schema(self.schema.id().clone());

        Arc::new(Self {
            schema: Arc::new(introspection_schema),
            fragments: self.fragments.clone(),
            selection_set: self.selection_set.clone(),
            shape_hash: self.shape_hash,
            kind: self.kind,
            network: self.network.clone(),
            logger: self.logger.clone(),
            start: self.start,
            query_text: self.query_text.clone(),
            variables_text: self.variables_text.clone(),
            query_id: self.query_id.clone(),
            complexity: self.complexity,
        })
    }

    /// Should only be called for fragments that exist in the query, and therefore have been
    /// validated to exist. Panics otherwise.
    pub fn get_fragment(&self, name: &str) -> &a::FragmentDefinition {
        self.fragments.get(name).unwrap()
    }

    /// Return `true` if this is a query, and not a subscription or
    /// mutation
    pub fn is_query(&self) -> bool {
        match self.kind {
            Kind::Query => true,
            Kind::Subscription => false,
        }
    }

    /// Return `true` if this is a subscription, not a query or a mutation
    pub fn is_subscription(&self) -> bool {
        match self.kind {
            Kind::Subscription => true,
            Kind::Query => false,
        }
    }

    /// Log details about the overall execution of the query
    pub fn log_execution(&self, block: BlockNumber) {
        if *graph::log::LOG_GQL_TIMING {
            info!(
                &self.logger,
                "Query timing (GraphQL)";
                "query" => &self.query_text,
                "variables" => &self.variables_text,
                "query_time_ms" => self.start.elapsed().as_millis(),
                "block" => block,
            );
        }
    }

    /// Log details about how the part of the query corresponding to
    /// `selection_set` was cached
    pub fn log_cache_status(
        &self,
        selection_set: &a::SelectionSet,
        block: BlockNumber,
        start: Instant,
        cache_status: String,
    ) {
        if *graph::log::LOG_GQL_CACHE_TIMING {
            info!(
                &self.logger,
                "Query caching";
                "query_time_ms" => start.elapsed().as_millis(),
                "cached" => cache_status,
                "selection" => %SelectedFields(selection_set),
                "block" => block,
            );
        }
    }
}

/// Coerces variable values for an operation.
pub fn coerce_variables(
    schema: &ApiSchema,
    operation: &q::OperationDefinition,
    mut variables: Option<QueryVariables>,
) -> Result<HashMap<String, r::Value>, Vec<QueryExecutionError>> {
    let mut coerced_values = HashMap::new();
    let mut errors = vec![];

    for variable_def in qast::get_variable_definitions(operation)
        .into_iter()
        .flatten()
    {
        // Skip variable if it has an invalid type
        if !sast::is_input_type(schema.document(), &variable_def.var_type) {
            errors.push(QueryExecutionError::InvalidVariableTypeError(
                variable_def.position,
                variable_def.name.to_owned(),
            ));
            continue;
        }

        let value = variables
            .as_mut()
            .and_then(|vars| vars.remove(&variable_def.name));

        let value = match value.or_else(|| {
            variable_def
                .default_value
                .clone()
                .map(r::Value::try_from)
                .transpose()
                .unwrap()
        }) {
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
            coerce_variable(schema, variable_def, value.into())?,
        );
    }

    if errors.is_empty() {
        Ok(coerced_values)
    } else {
        Err(errors)
    }
}

fn coerce_variable(
    schema: &ApiSchema,
    variable_def: &q::VariableDefinition,
    value: r::Value,
) -> Result<r::Value, Vec<QueryExecutionError>> {
    use crate::values::coercion::coerce_value;

    let resolver = |name: &str| schema.document().get_named_type(name);

    coerce_value(value, &variable_def.var_type, &resolver).map_err(|value| {
        vec![QueryExecutionError::InvalidArgumentError(
            variable_def.position,
            variable_def.name.to_owned(),
            value.into(),
        )]
    })
}

struct RawQuery {
    /// The schema against which to execute the query
    schema: Arc<ApiSchema>,
    /// The variables for the query, coerced into proper values
    variables: HashMap<String, r::Value>,
    /// The root selection set of the query
    selection_set: q::SelectionSet,

    fragments: HashMap<String, q::FragmentDefinition>,
}

impl RawQuery {
    fn check_complexity(
        &self,
        max_complexity: Option<u64>,
        max_depth: u8,
    ) -> Result<u64, Vec<QueryExecutionError>> {
        let complexity = self.complexity(max_depth).map_err(|e| vec![e])?;
        if let Some(max_complexity) = max_complexity {
            if complexity > max_complexity {
                return Err(vec![QueryExecutionError::TooComplex(
                    complexity,
                    max_complexity,
                )]);
            }
        }
        Ok(complexity)
    }

    fn complexity_inner<'a>(
        &'a self,
        ty: &s::TypeDefinition,
        selection_set: &'a q::SelectionSet,
        max_depth: u8,
        depth: u8,
        visited_fragments: &'a HashSet<&'a str>,
    ) -> Result<u64, ComplexityError> {
        use ComplexityError::*;

        if depth >= max_depth {
            return Err(TooDeep);
        }

        selection_set
            .items
            .iter()
            .try_fold(0, |total_complexity, selection| {
                let schema = self.schema.document();
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

                        let field_complexity = self.complexity_inner(
                            &get_named_type(schema, s_field.field_type.get_base_type())
                                .ok_or(Invalid)?,
                            &field.selection_set,
                            max_depth,
                            depth + 1,
                            visited_fragments,
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
                        let def = self.fragments.get(&fragment.fragment_name).unwrap();
                        let q::TypeCondition::On(type_name) = &def.type_condition;
                        let ty = get_named_type(schema, &type_name).ok_or(Invalid)?;

                        // Copy `visited_fragments` on write.
                        let mut visited_fragments = visited_fragments.clone();
                        if !visited_fragments.insert(&fragment.fragment_name) {
                            return Err(CyclicalFragment(fragment.fragment_name.clone()));
                        }
                        self.complexity_inner(
                            &ty,
                            &def.selection_set,
                            max_depth,
                            depth + 1,
                            &visited_fragments,
                        )
                    }
                    q::Selection::InlineFragment(fragment) => {
                        let ty = match &fragment.type_condition {
                            Some(q::TypeCondition::On(type_name)) => {
                                get_named_type(schema, &type_name).ok_or(Invalid)?
                            }
                            _ => ty.clone(),
                        };
                        self.complexity_inner(
                            &ty,
                            &fragment.selection_set,
                            max_depth,
                            depth + 1,
                            visited_fragments,
                        )
                    }
                }
                .and_then(|complexity| total_complexity.checked_add(complexity).ok_or(Overflow))
            })
    }

    /// See https://developer.github.com/v4/guides/resource-limitations/.
    ///
    /// If the query is invalid, returns `Ok(0)` so that execution proceeds and
    /// gives a proper error.
    fn complexity(&self, max_depth: u8) -> Result<u64, QueryExecutionError> {
        let root_type = sast::get_root_query_type_def(self.schema.document()).unwrap();

        match self.complexity_inner(
            root_type,
            &self.selection_set,
            max_depth,
            0,
            &HashSet::new(),
        ) {
            Ok(complexity) => Ok(complexity),
            Err(ComplexityError::Invalid) => Ok(0),
            Err(ComplexityError::TooDeep) => Err(QueryExecutionError::TooDeep(max_depth)),
            Err(ComplexityError::Overflow) => {
                Err(QueryExecutionError::TooComplex(u64::max_value(), 0))
            }
            Err(ComplexityError::CyclicalFragment(name)) => {
                Err(QueryExecutionError::CyclicalFragment(name))
            }
        }
    }

    fn validate_fields(&self) -> Result<(), Vec<QueryExecutionError>> {
        let root_type = self.schema.document().get_root_query_type().unwrap();

        let errors =
            self.validate_fields_inner(&"Query".to_owned(), root_type.into(), &self.selection_set);
        if errors.len() == 0 {
            Ok(())
        } else {
            Err(errors)
        }
    }

    // Checks for invalid selections.
    fn validate_fields_inner(
        &self,
        type_name: &str,
        ty: ObjectOrInterface<'_>,
        selection_set: &q::SelectionSet,
    ) -> Vec<QueryExecutionError> {
        let schema = self.schema.document();

        selection_set
            .items
            .iter()
            .fold(vec![], |mut errors, selection| {
                match selection {
                    q::Selection::Field(field) => match get_field(ty, &field.name) {
                        Some(s_field) => {
                            let base_type = s_field.field_type.get_base_type();
                            if get_named_type(schema, base_type).is_none() {
                                errors.push(QueryExecutionError::NamedTypeError(base_type.into()));
                            } else if let Some(ty) = object_or_interface(schema, base_type) {
                                errors.extend(self.validate_fields_inner(
                                    base_type,
                                    ty,
                                    &field.selection_set,
                                ))
                            }
                        }
                        None => errors.push(QueryExecutionError::UnknownField(
                            field.position,
                            type_name.into(),
                            field.name.clone(),
                        )),
                    },
                    q::Selection::FragmentSpread(fragment) => {
                        match self.fragments.get(&fragment.fragment_name) {
                            Some(frag) => {
                                let q::TypeCondition::On(type_name) = &frag.type_condition;
                                match object_or_interface(schema, type_name) {
                                    Some(ty) => errors.extend(self.validate_fields_inner(
                                        type_name,
                                        ty,
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
                            match object_or_interface(schema, type_name) {
                                Some(ty) => errors.extend(self.validate_fields_inner(
                                    type_name,
                                    ty,
                                    &fragment.selection_set,
                                )),
                                None => errors
                                    .push(QueryExecutionError::NamedTypeError(type_name.clone())),
                            }
                        }
                        _ => errors.extend(self.validate_fields_inner(
                            type_name,
                            ty,
                            &fragment.selection_set,
                        )),
                    },
                }
                errors
            })
    }

    fn expand_variables(
        self,
    ) -> Result<(a::SelectionSet, HashMap<String, a::FragmentDefinition>), QueryExecutionError>
    {
        fn expand_field(
            field: q::Field,
            vars: &HashMap<String, r::Value>,
        ) -> Result<a::Field, QueryExecutionError> {
            let q::Field {
                position,
                alias,
                name,
                arguments,
                directives,
                selection_set,
            } = field;
            let arguments = expand_arguments(arguments, &position, vars)?;
            let directives = expand_directives(directives, vars)?;
            let selection_set = expand_selection_set(selection_set, vars)?;
            Ok(a::Field {
                position,
                alias,
                name,
                arguments,
                directives,
                selection_set,
            })
        }

        fn expand_selection_set(
            set: q::SelectionSet,
            vars: &HashMap<String, r::Value>,
        ) -> Result<a::SelectionSet, QueryExecutionError> {
            let q::SelectionSet { span, items } = set;
            let items = items
                .into_iter()
                .map(|sel| match sel {
                    q::Selection::Field(field) => {
                        expand_field(field, vars).map(a::Selection::Field)
                    }
                    q::Selection::FragmentSpread(spread) => {
                        let q::FragmentSpread {
                            position,
                            fragment_name,
                            directives,
                        } = spread;
                        expand_directives(directives, vars).map(|directives| {
                            a::Selection::FragmentSpread(a::FragmentSpread {
                                position,
                                fragment_name,
                                directives,
                            })
                        })
                    }
                    q::Selection::InlineFragment(frag) => {
                        let q::InlineFragment {
                            position,
                            type_condition,
                            directives,
                            selection_set,
                        } = frag;
                        expand_directives(directives, vars).and_then(|directives| {
                            expand_selection_set(selection_set, vars).map(|selection_set| {
                                let type_condition = type_condition.map(|type_condition| {
                                    let q::TypeCondition::On(name) = type_condition;
                                    a::TypeCondition::On(name)
                                });
                                a::Selection::InlineFragment(a::InlineFragment {
                                    position,
                                    type_condition,
                                    directives,
                                    selection_set,
                                })
                            })
                        })
                    }
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(a::SelectionSet::new(span, items))
        }

        fn expand_directives(
            dirs: Vec<q::Directive>,
            vars: &HashMap<String, r::Value>,
        ) -> Result<Vec<a::Directive>, QueryExecutionError> {
            dirs.into_iter()
                .map(|dir| {
                    let q::Directive {
                        name,
                        position,
                        arguments,
                    } = dir;
                    expand_arguments(arguments, &position, vars).map(|arguments| a::Directive {
                        name,
                        position,
                        arguments,
                    })
                })
                .collect()
        }

        fn expand_arguments(
            args: Vec<(String, q::Value)>,
            pos: &Pos,
            vars: &HashMap<String, r::Value>,
        ) -> Result<Vec<(String, r::Value)>, QueryExecutionError> {
            args.into_iter()
                .map(|(name, val)| expand_value(val, pos, vars).map(|val| (name, val)))
                .collect()
        }

        fn expand_value(
            value: q::Value,
            pos: &Pos,
            vars: &HashMap<String, r::Value>,
        ) -> Result<r::Value, QueryExecutionError> {
            match value {
                q::Value::Variable(var) => match vars.get(&var) {
                    Some(val) => Ok(val.clone()),
                    None => Err(QueryExecutionError::MissingVariableError(
                        pos.clone(),
                        var.to_string(),
                    )),
                },
                q::Value::Int(ref num) => Ok(r::Value::Int(
                    num.as_i64().expect("q::Value::Int contains an i64"),
                )),
                q::Value::Float(f) => Ok(r::Value::Float(f)),
                q::Value::String(s) => Ok(r::Value::String(s)),
                q::Value::Boolean(b) => Ok(r::Value::Boolean(b)),
                q::Value::Null => Ok(r::Value::Null),
                q::Value::Enum(s) => Ok(r::Value::Enum(s)),
                q::Value::List(vals) => {
                    let vals: Vec<_> = vals
                        .into_iter()
                        .map(|val| expand_value(val, pos, vars))
                        .collect::<Result<Vec<_>, _>>()?;
                    Ok(r::Value::List(vals))
                }
                q::Value::Object(map) => {
                    let mut rmap = BTreeMap::new();
                    for (key, value) in map.into_iter() {
                        let value = expand_value(value, pos, vars)?;
                        rmap.insert(key, value);
                    }
                    Ok(r::Value::object(rmap))
                }
            }
        }

        fn expand_fragments(
            fragments: HashMap<String, q::FragmentDefinition>,
            vars: &HashMap<String, r::Value>,
        ) -> Result<HashMap<String, a::FragmentDefinition>, QueryExecutionError> {
            let mut new_fragments = HashMap::new();
            for (type_name, def) in fragments {
                let q::FragmentDefinition {
                    position,
                    name,
                    type_condition,
                    directives,
                    selection_set,
                } = def;
                let directives = expand_directives(directives, vars)?;
                let selection_set = expand_selection_set(selection_set, vars)?;
                let def = a::FragmentDefinition {
                    position,
                    name,
                    type_condition: type_condition.into(),
                    directives,
                    selection_set,
                };
                new_fragments.insert(type_name, def);
            }
            Ok(new_fragments)
        }

        let RawQuery {
            schema: _,
            variables,
            selection_set,
            fragments,
        } = self;
        expand_selection_set(selection_set, &variables).and_then(|selection_set| {
            expand_fragments(fragments, &variables).map(|fragments| (selection_set, fragments))
        })
    }
}
