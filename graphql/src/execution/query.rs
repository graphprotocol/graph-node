use graphql_parser::query as q;
use graphql_parser::schema as s;
use std::collections::HashMap;
use std::sync::Arc;

use graph::data::graphql::ext::TypeExt;
use graph::data::query::{Query as GraphDataQuery, QueryVariables};
use graph::data::schema::Schema;
use graph::prelude::QueryExecutionError;

use crate::execution::{get_field, get_named_type};
use crate::introspection::introspection_schema;
use crate::query::ast as qast;
use crate::schema::ast as sast;

#[derive(Copy, Clone, Debug)]
pub enum ComplexityError {
    TooDeep,
    Overflow,
    Invalid,
}

#[derive(Copy, Clone)]
enum Kind {
    Query,
    Subscription,
    Either,
}

pub struct Query {
    pub schema: Arc<Schema>,
    pub variables: HashMap<q::Name, q::Value>,
    pub selection_set: q::SelectionSet,
    fragments: HashMap<String, q::FragmentDefinition>,
    kind: Kind,
    pub verify: bool,
}

impl Query {
    /// Process the raw GraphQL query `query` and prepare for executing it.
    /// The returned `Query` has already been validated and, if `max_complexity`
    /// is given, also checked whether it is too complex. If validation fails,
    /// or the query is too complex, errors are returned
    pub fn new(
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

        let verify = if let q::OperationDefinition::Query(query) = &operation {
            query.directives.iter().any(|dir| dir.name == "verify")
        } else {
            false
        };

        let variables = coerce_variables(&query.schema, &operation, &query.variables)?;
        let (kind, selection_set) = match operation {
            q::OperationDefinition::Query(q::Query { selection_set, .. }) => {
                (Kind::Query, selection_set)
            }
            q::OperationDefinition::SelectionSet(selection_set) => (Kind::Either, selection_set),
            q::OperationDefinition::Subscription(q::Subscription { selection_set, .. }) => {
                (Kind::Subscription, selection_set)
            }
            q::OperationDefinition::Mutation(_) => {
                return Err(vec![QueryExecutionError::NotSupported(
                    "Mutations are not supported".to_owned(),
                )])
            }
        };

        let query = Arc::new(Self {
            schema: query.schema,
            variables,
            fragments,
            selection_set,
            kind,
            verify,
        });

        query.validate_fields()?;
        query.check_complexity(max_complexity, max_depth)?;

        Ok(query)
    }

    pub fn as_introspection_query(&self) -> Arc<Self> {
        let introspection_schema = introspection_schema(self.schema.id.clone());

        Arc::new(Self {
            schema: Arc::new(introspection_schema),
            variables: self.variables.clone(),
            fragments: self.fragments.clone(),
            selection_set: self.selection_set.clone(),
            kind: self.kind,
            verify: self.verify,
        })
    }

    pub fn get_fragment(&self, name: &q::Name) -> Option<&q::FragmentDefinition> {
        self.fragments.get(name)
    }

    pub fn is_query(&self) -> bool {
        match self.kind {
            Kind::Query | Kind::Either => true,
            Kind::Subscription => false,
        }
    }

    pub fn is_subscription(&self) -> bool {
        match self.kind {
            Kind::Subscription => true,
            Kind::Query | Kind::Either => false,
        }
    }

    fn check_complexity(
        &self,
        max_complexity: Option<u64>,
        max_depth: u8,
    ) -> Result<(), Vec<QueryExecutionError>> {
        if let Some(max_complexity) = max_complexity {
            let complexity = self.complexity(max_depth).map_err(|e| vec![e])?;
            if complexity > max_complexity {
                return Err(vec![QueryExecutionError::TooComplex(
                    complexity,
                    max_complexity,
                )]);
            }
        }
        Ok(())
    }

    /// See https://developer.github.com/v4/guides/resource-limitations/.
    ///
    /// If the query is invalid, returns `Ok(0)` so that execution proceeds and
    /// gives a proper error.
    fn complexity(&self, max_depth: u8) -> Result<u64, QueryExecutionError> {
        let root_type = sast::get_root_query_type_def(&self.schema.document).unwrap();

        match self.complexity_inner(root_type, &self.selection_set, max_depth, 0) {
            Ok(complexity) => Ok(complexity),
            Err(ComplexityError::Invalid) => Ok(0),
            Err(ComplexityError::TooDeep) => Err(QueryExecutionError::TooDeep(max_depth)),
            Err(ComplexityError::Overflow) => {
                Err(QueryExecutionError::TooComplex(u64::max_value(), 0))
            }
        }
    }

    fn validate_fields(&self) -> Result<(), Vec<QueryExecutionError>> {
        let root_type = sast::get_root_query_type_def(&self.schema.document).unwrap();

        let errors =
            self.validate_fields_inner(&"Query".to_owned(), root_type, &self.selection_set);
        if errors.len() == 0 {
            Ok(())
        } else {
            Err(errors)
        }
    }

    // Checks for invalid selections.
    fn validate_fields_inner(
        &self,
        type_name: &q::Name,
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
                                    Some(ty) => errors.extend(self.validate_fields_inner(
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
                        match self.get_fragment(&fragment.fragment_name) {
                            Some(frag) => {
                                let q::TypeCondition::On(type_name) = &frag.type_condition;
                                match get_named_type(schema, type_name) {
                                    Some(ty) => errors.extend(self.validate_fields_inner(
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
                                Some(ty) => errors.extend(self.validate_fields_inner(
                                    type_name,
                                    &ty,
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

    fn complexity_inner(
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

                        let field_complexity = self.complexity_inner(
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
                        let def = self.get_fragment(&fragment.fragment_name).ok_or(Invalid)?;
                        let q::TypeCondition::On(type_name) = &def.type_condition;
                        let ty = get_named_type(schema, &type_name).ok_or(Invalid)?;
                        self.complexity_inner(&ty, &def.selection_set, max_depth, depth + 1)
                    }
                    q::Selection::InlineFragment(fragment) => {
                        let ty = match &fragment.type_condition {
                            Some(q::TypeCondition::On(type_name)) => {
                                get_named_type(schema, &type_name).ok_or(Invalid)?
                            }
                            _ => ty.clone(),
                        };
                        self.complexity_inner(&ty, &fragment.selection_set, max_depth, depth + 1)
                    }
                }
                .and_then(|complexity| total_complexity.checked_add(complexity).ok_or(Overflow))
            })
    }
}

/// Coerces variable values for an operation.
pub fn coerce_variables(
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
            coerce_variable(schema, variable_def, &value)?,
        );
    }

    if errors.is_empty() {
        Ok(coerced_values)
    } else {
        Err(errors)
    }
}

fn coerce_variable(
    schema: &Schema,
    variable_def: &q::VariableDefinition,
    value: &q::Value,
) -> Result<q::Value, Vec<QueryExecutionError>> {
    use crate::values::coercion::coerce_value;

    let resolver = |name: &q::Name| sast::get_named_type(&schema.document, name);

    coerce_value(&value, &variable_def.var_type, &resolver, &HashMap::new()).ok_or_else(|| {
        vec![QueryExecutionError::InvalidArgumentError(
            variable_def.position,
            variable_def.name.to_owned(),
            value.clone(),
        )]
    })
}
