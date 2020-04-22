use graphql_parser::query as q;
use graphql_parser::schema as s;
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

pub struct Query {
    pub schema: Arc<Schema>,
    pub document: q::Document,
    pub variables: Option<QueryVariables>,
}

impl Query {
    pub fn new(query: GraphDataQuery) -> Result<Arc<Self>, QueryExecutionError> {
        Ok(Arc::new(Self {
            schema: query.schema,
            document: query.document,
            variables: query.variables,
        }))
    }

    pub fn as_introspection_query(&self) -> Arc<Self> {
        let introspection_schema = introspection_schema(self.schema.id.clone());

        Arc::new(Self {
            schema: Arc::new(introspection_schema),
            document: self.document.clone(),
            variables: self.variables.clone(),
        })
    }

    /// See https://developer.github.com/v4/guides/resource-limitations/.
    ///
    /// If the query is invalid, returns `Ok(0)` so that execution proceeds and
    /// gives a proper error.
    pub fn complexity(
        &self,
        selection_set: &q::SelectionSet,
        max_depth: u8,
    ) -> Result<u64, QueryExecutionError> {
        let root_type = sast::get_root_query_type_def(&self.schema.document).unwrap();

        match self.complexity_inner(root_type, selection_set, max_depth, 0) {
            Ok(complexity) => Ok(complexity),
            Err(ComplexityError::Invalid) => Ok(0),
            Err(ComplexityError::TooDeep) => Err(QueryExecutionError::TooDeep(max_depth)),
            Err(ComplexityError::Overflow) => {
                Err(QueryExecutionError::TooComplex(u64::max_value(), 0))
            }
        }
    }

    pub fn validate_fields(&self, selection_set: &q::SelectionSet) -> Vec<QueryExecutionError> {
        let root_type = sast::get_root_query_type_def(&self.schema.document).unwrap();

        self.validate_fields_inner(&"Query".to_owned(), root_type, selection_set)
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
                        match qast::get_fragment(&self.document, &fragment.fragment_name) {
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
                        let def = qast::get_fragment(&self.document, &fragment.fragment_name)
                            .ok_or(Invalid)?;
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
