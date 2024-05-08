use std::collections::{BTreeSet, HashSet};

use graph::{
    components::store::{AttributeNames, ChildMultiplicity, EntityOrder},
    data::{graphql::QueryableType, store::ID},
    env::ENV_VARS,
    prelude::{anyhow, q, r, s, QueryExecutionError, ValueMap},
    schema::{ast::ObjectType, kw, AggregationInterval, ApiSchema, EntityType},
};

/// A selection set is a table that maps object types to the fields that
/// should be selected for objects of that type. The types are always
/// concrete object types, never interface or union types. When a
/// `SelectionSet` is constructed, fragments must already have been resolved
/// as it only allows using fields.
///
/// The set of types that a `SelectionSet` can accommodate must be set at
/// the time the `SelectionSet` is constructed. It is not possible to add
/// more types to it, but it is possible to add fields for all known types
/// or only some of them
#[derive(Debug, Clone, PartialEq)]
pub struct SelectionSet {
    // Map object types to the list of fields that should be selected for
    // them. In most cases, this will have a single entry. If the
    // `SelectionSet` is attached to a field with an interface or union
    // type, it will have an entry for each object type implementing that
    // interface or being part of the union
    items: Vec<(ObjectType, Vec<Field>)>,
}

impl SelectionSet {
    /// Create a new `SelectionSet` that can handle the given types
    pub fn new(types: Vec<ObjectType>) -> Self {
        let items = types
            .into_iter()
            .map(|obj_type| (obj_type, Vec::new()))
            .collect();
        SelectionSet { items }
    }

    /// Create a new `SelectionSet` that can handle the same types as
    /// `other`, but ignore all fields from `other`
    pub fn empty_from(other: &SelectionSet) -> Self {
        let items = other
            .items
            .iter()
            .map(|(name, _)| (name.clone(), Vec::new()))
            .collect();
        SelectionSet { items }
    }

    /// Return `true` if this selection set does not select any fields for
    /// its types
    pub fn is_empty(&self) -> bool {
        self.items.iter().all(|(_, fields)| fields.is_empty())
    }

    /// If the selection set contains a single field across all its types,
    /// return it. Otherwise, return `None`
    pub fn single_field(&self) -> Option<&Field> {
        let mut iter = self.items.iter();
        let field = match iter.next() {
            Some((_, fields)) => {
                if fields.len() != 1 {
                    return None;
                } else {
                    &fields[0]
                }
            }
            None => return None,
        };
        for (_, fields) in iter {
            if fields.len() != 1 {
                return None;
            }
            if &fields[0] != field {
                return None;
            }
        }
        Some(field)
    }

    /// Iterate over all types and the fields for those types
    pub fn fields(&self) -> impl Iterator<Item = (&ObjectType, impl Iterator<Item = &Field>)> {
        self.items
            .iter()
            .map(|(obj_type, fields)| (obj_type, fields.iter()))
    }

    /// Iterate over all types and the fields that are not leaf fields, i.e.
    /// whose selection sets are not empty
    pub fn interior_fields(
        &self,
    ) -> impl Iterator<Item = (&ObjectType, impl Iterator<Item = &Field>)> {
        self.items
            .iter()
            .map(|(obj_type, fields)| (obj_type, fields.iter().filter(|field| !field.is_leaf())))
    }

    /// Iterate over all fields for the given object type
    pub fn fields_for(
        &self,
        obj_type: &ObjectType,
    ) -> Result<impl Iterator<Item = &Field>, QueryExecutionError> {
        self.fields_for_name(&obj_type.name)
    }

    fn fields_for_name(
        &self,
        name: &str,
    ) -> Result<impl Iterator<Item = &Field>, QueryExecutionError> {
        let item = self
            .items
            .iter()
            .find(|(our_type, _)| our_type.name == name)
            .ok_or_else(|| {
                // see: graphql-bug-compat
                // Once queries are validated, this can become a panic since
                // users won't be able to trigger this any more
                QueryExecutionError::ValidationError(
                    None,
                    format!("invalid query: no fields for type `{}`", name),
                )
            })?;
        Ok(item.1.iter())
    }

    /// Append the field for all the sets' types
    pub fn push(&mut self, new_field: &Field) -> Result<(), QueryExecutionError> {
        for (_, fields) in &mut self.items {
            Self::merge_field(fields, new_field.clone())?;
        }
        Ok(())
    }

    /// Append the fields for all the sets' types
    pub fn push_fields(&mut self, fields: Vec<&Field>) -> Result<(), QueryExecutionError> {
        for field in fields {
            self.push(field)?;
        }
        Ok(())
    }

    /// Merge `self` with the fields from `other`, which must have the same,
    /// or a subset of, the types of `self`. The `directives` are added to
    /// `self`'s directives so that they take precedence over existing
    /// directives with the same name
    pub fn merge(
        &mut self,
        other: SelectionSet,
        directives: Vec<Directive>,
    ) -> Result<(), QueryExecutionError> {
        for (other_type, other_fields) in other.items {
            let item = self
                .items
                .iter_mut()
                .find(|(obj_type, _)| &other_type == obj_type)
                .ok_or_else(|| {
                    // graphql-bug-compat: once queries are validated, this
                    // can become a panic since users won't be able to
                    // trigger this anymore
                    QueryExecutionError::ValidationError(
                        None,
                        format!(
                            "invalid query: can not merge fields because type `{}` showed up unexpectedly",
                            other_type.name
                        ),
                    )
                })?;
            for mut other_field in other_fields {
                other_field.prepend_directives(directives.clone());
                Self::merge_field(&mut item.1, other_field)?;
            }
        }
        Ok(())
    }

    fn merge_field(fields: &mut Vec<Field>, new_field: Field) -> Result<(), QueryExecutionError> {
        match fields
            .iter_mut()
            .find(|field| field.response_key() == new_field.response_key())
        {
            Some(field) => {
                // TODO: check that _field and new_field are mergeable, in
                // particular that their name, directives and arguments are
                // compatible
                field.selection_set.merge(new_field.selection_set, vec![])?;
            }
            None => fields.push(new_field),
        }
        Ok(())
    }

    /// Dump the selection set as a string for debugging
    #[cfg(debug_assertions)]
    pub fn dump(&self) -> String {
        fn dump_selection_set(selection_set: &SelectionSet, indent: usize, out: &mut String) {
            for (object_type, fields) in selection_set.interior_fields() {
                for field in fields {
                    for _ in 0..indent {
                        out.push(' ');
                    }
                    let intv = field
                        .aggregation_interval()
                        .unwrap()
                        .map(|intv| format!("[{intv}]"))
                        .unwrap_or_default();
                    out.push_str(&format!("{}: {}{intv}\n", object_type.name, field.name));
                    dump_selection_set(&field.selection_set, indent + 2, out);
                }
            }
        }
        let mut out = String::new();
        dump_selection_set(self, 0, &mut out);
        out
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Directive {
    pub position: q::Pos,
    pub name: String,
    pub arguments: Vec<(String, r::Value)>,
}

impl Directive {
    /// Looks up the value of an argument of this directive
    pub fn argument_value(&self, name: &str) -> Option<&r::Value> {
        self.arguments
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, v)| v)
    }

    fn eval_if(&self) -> bool {
        match self.argument_value("if") {
            None => true,
            Some(r::Value::Boolean(b)) => *b,
            Some(_) => false,
        }
    }

    /// Return `true` if this directive says that we should not include the
    /// field it is attached to. That is the case if the directive is
    /// `include` and its `if` condition is `false`, or if it is `skip` and
    /// its `if` condition is `true`. In all other cases, return `false`
    pub fn skip(&self) -> bool {
        match self.name.as_str() {
            "include" => !self.eval_if(),
            "skip" => self.eval_if(),
            _ => false,
        }
    }
}

/// A field to execute as part of a query. When the field is constructed by
/// `Query::new`, variables are interpolated, and argument values have
/// already been coerced to the appropriate types for the field argument
#[derive(Debug, Clone, PartialEq)]
pub struct Field {
    pub position: q::Pos,
    pub alias: Option<String>,
    pub name: String,
    pub arguments: Vec<(String, r::Value)>,
    pub directives: Vec<Directive>,
    pub selection_set: SelectionSet,
    pub multiplicity: ChildMultiplicity,
}

impl Field {
    /// Returns the response key of a field, which is either its name or its
    /// alias (if there is one).
    pub fn response_key(&self) -> &str {
        self.alias.as_deref().unwrap_or(self.name.as_str())
    }

    /// Looks up the value of an argument for this field
    pub fn argument_value(&self, name: &str) -> Option<&r::Value> {
        self.arguments
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, v)| v)
    }

    fn prepend_directives(&mut self, mut directives: Vec<Directive>) {
        // TODO: check that the new directives don't conflict with existing
        // directives
        std::mem::swap(&mut self.directives, &mut directives);
        self.directives.extend(directives);
    }

    fn is_leaf(&self) -> bool {
        self.selection_set.is_empty()
    }

    /// Return the set of attributes that should be selected for this field.
    /// If `ENV_VARS.enable_select_by_specific_attributes` is `false`,
    /// return `AttributeNames::All
    pub fn selected_attrs(
        &self,
        entity_type: &EntityType,
        order: &EntityOrder,
    ) -> Result<AttributeNames, QueryExecutionError> {
        if !ENV_VARS.enable_select_by_specific_attributes {
            return Ok(AttributeNames::All);
        }

        let fields = self.selection_set.fields_for_name(entity_type.typename())?;

        // Extract the attributes we should select from `selection_set`. In
        // particular, disregard derived fields since they are not stored
        let mut column_names: BTreeSet<String> = fields
            .filter(|field| {
                // Keep fields that are not derived and for which we
                // can find the field type
                entity_type
                    .field(&field.name)
                    .map_or(false, |field| !field.is_derived())
            })
            .filter_map(|field| {
                if field.name.starts_with("__") {
                    None
                } else {
                    Some(field.name.clone())
                }
            })
            .collect();

        // We need to also select the `orderBy` field if there is one
        use EntityOrder::*;
        let order_field = match order {
            Ascending(name, _) | Descending(name, _) => Some(name.as_str()),
            Default => Some(ID.as_str()),
            ChildAscending(_) | ChildDescending(_) | Unordered => {
                // No need to select anything for these
                None
            }
        };
        if let Some(order_field) = order_field {
            // We assume that `order` only contains valid field names
            column_names.insert(order_field.to_string());
        }
        Ok(AttributeNames::Select(column_names))
    }

    /// Return the value of the `interval` argument if there is one. Return
    /// `None` if the argument is not present, and an error if the argument
    /// is present but can not be parsed as an `AggregationInterval`
    pub fn aggregation_interval(&self) -> Result<Option<AggregationInterval>, QueryExecutionError> {
        self.argument_value(kw::INTERVAL)
            .map(|value| match value {
                r::Value::Enum(interval) => interval.parse::<AggregationInterval>().map_err(|_| {
                    QueryExecutionError::InvalidArgumentError(
                        self.position.clone(),
                        kw::INTERVAL.to_string(),
                        q::Value::from(value.clone()),
                    )
                }),
                _ => Err(QueryExecutionError::InvalidArgumentError(
                    self.position.clone(),
                    kw::INTERVAL.to_string(),
                    q::Value::from(value.clone()),
                )),
            })
            .transpose()
    }
}

impl ValueMap for Field {
    fn get_required<T: graph::prelude::TryFromValue>(&self, key: &str) -> Result<T, anyhow::Error> {
        self.argument_value(key)
            .ok_or_else(|| anyhow!("Required field `{}` not set", key))
            .and_then(T::try_from_value)
    }

    fn get_optional<T: graph::prelude::TryFromValue>(
        &self,
        key: &str,
    ) -> Result<Option<T>, anyhow::Error> {
        self.argument_value(key)
            .map_or(Ok(None), |value| match value {
                r::Value::Null => Ok(None),
                _ => T::try_from_value(value).map(Some),
            })
    }
}

/// A set of object types, generated from resolving interfaces into the
/// object types that implement them, and possibly narrowing further when
/// expanding fragments with type conditions
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ObjectTypeSet {
    Any,
    Only(HashSet<ObjectType>),
}

impl ObjectTypeSet {
    pub fn convert(
        schema: &ApiSchema,
        type_cond: Option<&q::TypeCondition>,
    ) -> Result<ObjectTypeSet, QueryExecutionError> {
        match type_cond {
            Some(q::TypeCondition::On(name)) => Self::from_name(schema, name),
            None => Ok(ObjectTypeSet::Any),
        }
    }

    pub fn from_name(schema: &ApiSchema, name: &str) -> Result<ObjectTypeSet, QueryExecutionError> {
        let set = resolve_object_types(schema, name)?;
        Ok(ObjectTypeSet::Only(set))
    }

    fn contains(&self, obj_type: &ObjectType) -> bool {
        match self {
            ObjectTypeSet::Any => true,
            ObjectTypeSet::Only(set) => set.contains(obj_type),
        }
    }

    pub fn intersect(self, other: &ObjectTypeSet) -> ObjectTypeSet {
        match self {
            ObjectTypeSet::Any => other.clone(),
            ObjectTypeSet::Only(set) => {
                ObjectTypeSet::Only(set.into_iter().filter(|ty| other.contains(ty)).collect())
            }
        }
    }

    /// Return a list of the object type names that are in this type set and
    /// are also implementations of `current_type`
    pub fn type_names(
        &self,
        schema: &ApiSchema,
        current_type: QueryableType<'_>,
    ) -> Result<Vec<ObjectType>, QueryExecutionError> {
        Ok(resolve_object_types(schema, current_type.name())?
            .into_iter()
            .filter(|obj_type| match self {
                ObjectTypeSet::Any => true,
                ObjectTypeSet::Only(set) => set.contains(obj_type),
            })
            .collect())
    }
}

/// Look up the type `name` from the schema and resolve interfaces
/// and unions until we are left with a set of concrete object types
pub(crate) fn resolve_object_types(
    schema: &ApiSchema,
    name: &str,
) -> Result<HashSet<ObjectType>, QueryExecutionError> {
    let mut set = HashSet::new();
    match schema
        .get_named_type(name)
        .ok_or_else(|| QueryExecutionError::AbstractTypeError(name.to_string()))?
    {
        s::TypeDefinition::Interface(intf) => {
            for obj_ty in &schema.types_for_interface_or_union()[&intf.name] {
                let obj_ty = schema.object_type(obj_ty);
                set.insert(obj_ty.into());
            }
        }
        s::TypeDefinition::Union(tys) => {
            for ty in &tys.types {
                set.extend(resolve_object_types(schema, ty)?)
            }
        }
        s::TypeDefinition::Object(ty) => {
            let ty = schema.object_type(ty);
            set.insert(ty.into());
        }
        s::TypeDefinition::Scalar(_)
        | s::TypeDefinition::Enum(_)
        | s::TypeDefinition::InputObject(_) => {
            return Err(QueryExecutionError::NamedTypeError(name.to_string()));
        }
    }
    Ok(set)
}
