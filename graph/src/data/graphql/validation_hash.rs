use crate::prelude::q;
use second_stack::buffer;
use std::cmp::Ordering;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

// The idea here is to avoid misses and increase the hit rate of the GraphQL validation's cache by normalizing GraphQL operations.
// An exact same hash for two queries means that they are identical in the context of GraphQL Validation rules.
//
// Hashing includes:
// - sorting of the fields, fragments, arguments, fragment definitions, variables etc.
// - ignoring operation names
// - transforming the selection sets like `query name { things }` into `{ things }`
// - removing primitive values (like String, Int, Float except Boolean because it can change the body of the operation when used with `@include` or `@skip`)
// - ignoring aliases
pub fn validation_hash(query: &q::Document) -> u64 {
    let mut hasher = DefaultHasher::new();
    query.query_validation_hash(&mut hasher);
    hasher.finish()
}

type QueryValidationHasher = DefaultHasher;

pub trait QueryValidationHash {
    fn query_validation_hash(&self, hasher: &mut QueryValidationHasher);
}

impl QueryValidationHash for q::Document {
    fn query_validation_hash(&self, hasher: &mut QueryValidationHasher) {
        buffer(self.definitions.iter(), |definitions| {
            // Sort definitions by kind
            definitions.sort_unstable_by(|a, b| compare_definitions(a, b));
            for definition in definitions {
                match definition {
                    q::Definition::Operation(operation) => operation.query_validation_hash(hasher),
                    q::Definition::Fragment(fragment) => fragment.query_validation_hash(hasher),
                }
            }
        });
    }
}

impl QueryValidationHash for q::OperationDefinition {
    fn query_validation_hash(&self, hasher: &mut QueryValidationHasher) {
        // We want `[query|subscription|mutation] things { BODY }` to hash
        // to the same thing as just `things { BODY }`, except variables
        // We ignore directives and operation names.
        match self {
            q::OperationDefinition::SelectionSet(set) => set.query_validation_hash(hasher),
            q::OperationDefinition::Query(query) => {
                // Sort variables by name
                buffer(query.variable_definitions.iter(), |variables| {
                    variables.sort_unstable_by(|a, b| compare_variable_definitions(a, b));

                    for variable in variables {
                        variable.query_validation_hash(hasher);
                    }
                });

                query.selection_set.query_validation_hash(hasher)
            }
            q::OperationDefinition::Mutation(mutation) => {
                // Sort variables by name
                buffer(mutation.variable_definitions.iter(), |variables| {
                    variables.sort_unstable_by(|a, b| compare_variable_definitions(a, b));

                    for variable in variables {
                        variable.query_validation_hash(hasher);
                    }
                });

                mutation.selection_set.query_validation_hash(hasher)
            }
            q::OperationDefinition::Subscription(subscription) => {
                // Sort variables by name
                buffer(subscription.variable_definitions.iter(), |variables| {
                    variables.sort_unstable_by(|a, b| compare_variable_definitions(a, b));

                    for variable in variables {
                        variable.query_validation_hash(hasher);
                    }
                });

                subscription.selection_set.query_validation_hash(hasher)
            }
        }
    }
}

impl QueryValidationHash for q::VariableDefinition {
    fn query_validation_hash(&self, hasher: &mut QueryValidationHasher) {
        self.name.hash(hasher);
        match &self.default_value {
            Some(value) => value.query_validation_hash(hasher),
            None => (),
        }
        self.var_type.query_validation_hash(hasher);
    }
}

impl QueryValidationHash for q::Type {
    fn query_validation_hash(&self, hasher: &mut QueryValidationHasher) {
        match self {
            q::Type::NamedType(name) => name.hash(hasher),
            q::Type::ListType(list) => {
                "list".hash(hasher);
                list.query_validation_hash(hasher)
            }
            q::Type::NonNullType(non_null) => {
                "non-null".hash(hasher);
                non_null.query_validation_hash(hasher)
            }
        }
    }
}

impl QueryValidationHash for q::FragmentDefinition {
    fn query_validation_hash(&self, hasher: &mut QueryValidationHasher) {
        self.name.hash(hasher);
        self.type_condition.query_validation_hash(hasher);
        self.selection_set.query_validation_hash(hasher);
    }
}

impl QueryValidationHash for q::SelectionSet {
    fn query_validation_hash(&self, hasher: &mut QueryValidationHasher) {
        buffer(self.items.iter(), |selections| {
            selections.sort_unstable_by(|a, b| compare_selections(a, b));

            for selection in selections {
                selection.query_validation_hash(hasher);
            }
        });
    }
}

impl QueryValidationHash for q::Selection {
    fn query_validation_hash(&self, hasher: &mut QueryValidationHasher) {
        // Skips directives
        match self {
            q::Selection::Field(field) => field.query_validation_hash(hasher),
            q::Selection::FragmentSpread(fragment) => fragment.fragment_name.hash(hasher),
            q::Selection::InlineFragment(fragment) => fragment.query_validation_hash(hasher),
        }
    }
}

impl QueryValidationHash for q::Field {
    fn query_validation_hash(&self, hasher: &mut QueryValidationHasher) {
        // Skips an alias and directives
        self.name.hash(hasher);

        buffer(self.arguments.iter(), |arguments| {
            arguments.sort_unstable_by(|a, b| compare_arguments(a, b));

            for arg in arguments {
                let (name, value) = arg;
                name.hash(hasher);
                value.query_validation_hash(hasher);
            }
        });

        self.selection_set.query_validation_hash(hasher);
    }
}

impl QueryValidationHash for q::InlineFragment {
    fn query_validation_hash(&self, hasher: &mut QueryValidationHasher) {
        // Skips directives
        match self.type_condition.clone() {
            Some(type_condition) => type_condition.query_validation_hash(hasher),
            None => (),
        }
        self.selection_set.query_validation_hash(hasher);
    }
}

impl QueryValidationHash for q::TypeCondition {
    fn query_validation_hash(&self, hasher: &mut QueryValidationHasher) {
        self.to_string().hash(hasher);
    }
}

impl QueryValidationHash for q::Value {
    fn query_validation_hash(&self, hasher: &mut QueryValidationHasher) {
        match self {
            q::Value::Variable(v) => {
                "variable".hash(hasher);
                v.hash(hasher);
            }
            // turns Int into 0
            q::Value::Int(_) => {
                "int".hash(hasher);
                0.hash(hasher)
            }
            // turns Float into 0
            q::Value::Float(_) => {
                "float".hash(hasher);
                0.hash(hasher)
            }
            // turns String into ""
            q::Value::String(_) => "".hash(hasher),
            // Do nothing for Boolean, because the value affect the body of the query
            // when `@include` or `@skip` directives are used
            q::Value::Boolean(b) => b.hash(hasher),
            q::Value::Enum(e) => {
                "enum".hash(hasher);
                e.hash(hasher)
            }
            q::Value::List(list) => {
                "list".hash(hasher);
                for item in list {
                    item.query_validation_hash(hasher);
                }
            }
            q::Value::Object(obj) => {
                "object".hash(hasher);
                for (key, value) in obj {
                    key.hash(hasher);
                    value.query_validation_hash(hasher);
                }
            }
            q::Value::Null => (),
        }
    }
}

fn compare_definitions<'a, T: q::Text<'a>>(
    a: &q::Definition<'a, T>,
    b: &q::Definition<'a, T>,
) -> Ordering {
    match (a, b) {
        // Keep operations as they are
        (q::Definition::Operation(_), q::Definition::Operation(_)) => Ordering::Equal,
        // Sort fragments by name
        (q::Definition::Fragment(a), q::Definition::Fragment(b)) => a.name.cmp(&b.name),
        // Operation -> Fragment
        _ => definition_kind_ordering(a).cmp(&definition_kind_ordering(b)),
    }
}

fn compare_selections<'a>(a: &q::Selection, b: &q::Selection) -> Ordering {
    match (a, b) {
        // Sort fields by name
        (q::Selection::Field(a), q::Selection::Field(b)) => a.name.cmp(&b.name),
        // Sort fragments by name
        (q::Selection::FragmentSpread(a), q::Selection::FragmentSpread(b)) => {
            a.fragment_name.cmp(&b.fragment_name)
        }
        _ => {
            let a_ordering = selection_kind_ordering(a);
            let b_ordering = selection_kind_ordering(b);
            a_ordering.cmp(&b_ordering)
        }
    }
}

fn compare_arguments<'a>(a: &(String, q::Value), b: &(String, q::Value)) -> Ordering {
    a.0.cmp(&b.0)
}

fn compare_variable_definitions<'a>(
    a: &q::VariableDefinition,
    b: &q::VariableDefinition,
) -> Ordering {
    a.name.cmp(&b.name)
}

/// Assigns an order to different variants of Selection
fn selection_kind_ordering<'a>(selection: &q::Selection) -> u8 {
    match selection {
        q::Selection::FragmentSpread(_) => 1,
        q::Selection::InlineFragment(_) => 2,
        q::Selection::Field(_) => 3,
    }
}

/// Assigns an order to different variants of Definition
fn definition_kind_ordering<'a, T: q::Text<'a>>(definition: &q::Definition<'a, T>) -> u8 {
    match definition {
        q::Definition::Operation(_) => 1,
        q::Definition::Fragment(_) => 2,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use graphql_parser::parse_query;

    #[test]
    fn sorted() {
        const Q1: &str = "{ things(first: 10, limit: 20) { e { ... on E { e1 e3 e2 } } f { ... F } a c b d } } fragment F on F { f1 f3 f2 }";
        const Q2: &str = "{ things(limit: 20, first: 10) { a c b d f { ...F } e { ... on E { e3 e1 e2 } } } } fragment F on F { f2 f3 f1 }";
        let q1 = parse_query(Q1)
            .expect("q1 is syntactically valid")
            .into_static();
        let q2 = parse_query(Q2)
            .expect("q2 is syntactically valid")
            .into_static();

        assert_eq!(validation_hash(&q1), validation_hash(&q2));
    }

    #[test]
    fn do_not_sort_inline_fragments() {
        const Q1: &str = "      { things { ... on ThingA { a }    ... on ThingB { b }    } }";
        const Q2: &str = "      { things { ... on ThingB { b }    ... on ThingA { a }    } }";
        const Q3: &str = "query { things { ... on ThingA { c: a } ... on ThingB { c: b } } }";
        const Q4: &str = "query { things { ... on ThingB { c: b } ... on ThingA { c: a } } }";
        let q1 = parse_query(Q1)
            .expect("q1 is syntactically valid")
            .into_static();
        let q2 = parse_query(Q2)
            .expect("q2 is syntactically valid")
            .into_static();
        let q3 = parse_query(Q3)
            .expect("q3 is syntactically valid")
            .into_static();
        let q4 = parse_query(Q4)
            .expect("q4 is syntactically valid")
            .into_static();

        assert_ne!(validation_hash(&q1), validation_hash(&q2));
        assert_ne!(validation_hash(&q3), validation_hash(&q4));
    }

    #[test]
    fn sort_fragment_spreads() {
        const Q1: &str =
            "{ things { ...A ...B } } fragment A on ThingsA { a } fragment B on ThingsB { b }";
        const Q2: &str =
            "{ things { ...B ...A } } fragment A on ThingsA { a } fragment B on ThingsB { b }";
        let q1 = parse_query(Q1)
            .expect("q1 is syntactically valid")
            .into_static();
        let q2 = parse_query(Q2)
            .expect("q2 is syntactically valid")
            .into_static();

        assert_eq!(validation_hash(&q1), validation_hash(&q2));
    }

    #[test]
    fn sort_fragment_definitions() {
        const Q1: &str =
            "{ things { ...A ...B } } fragment B on ThingsB { b } fragment A on ThingsA { a } ";
        const Q2: &str =
            "{ things { ...A ...B } } fragment A on ThingsA { a } fragment B on ThingsB { b }";
        let q1 = parse_query(Q1)
            .expect("q1 is syntactically valid")
            .into_static();
        let q2 = parse_query(Q2)
            .expect("q2 is syntactically valid")
            .into_static();

        assert_eq!(validation_hash(&q1), validation_hash(&q2));
    }

    #[test]
    fn literals_and_operation_name() {
        const Q1: &str = "query a { things(where: { stuff_gt: 20 }) { a c b d } }";
        const Q2: &str = "query b { things(where: { stuff_gt: 30 }) { a c b d } }";
        let q1 = parse_query(Q1)
            .expect("q1 is syntactically valid")
            .into_static();
        let q2 = parse_query(Q2)
            .expect("q2 is syntactically valid")
            .into_static();

        assert_eq!(validation_hash(&q1), validation_hash(&q2));
    }

    #[test]
    fn selection_set_into_query() {
        const Q1: &str = "query a { things { a c b d } }";
        const Q2: &str = "        { things { a c b d } }";
        let q1 = parse_query(Q1)
            .expect("q1 is syntactically valid")
            .into_static();
        let q2 = parse_query(Q2)
            .expect("q2 is syntactically valid")
            .into_static();

        assert_eq!(validation_hash(&q1), validation_hash(&q2));
    }

    #[test]
    fn extra_field() {
        const Q1: &str = "{ things { a c b d e } }";
        const Q2: &str = "{ things { a c b d   } }";
        let q1 = parse_query(Q1)
            .expect("q1 is syntactically valid")
            .into_static();
        let q2 = parse_query(Q2)
            .expect("q2 is syntactically valid")
            .into_static();

        assert_ne!(validation_hash(&q1), validation_hash(&q2));
    }

    #[test]
    fn aliases() {
        const Q1: &str = "{ things {    a c b d e } }";
        const Q2: &str = "{ things { aa:a c b d e } }";
        let q1 = parse_query(Q1)
            .expect("q1 is syntactically valid")
            .into_static();
        let q2 = parse_query(Q2)
            .expect("q2 is syntactically valid")
            .into_static();

        assert_eq!(validation_hash(&q1), validation_hash(&q2));
    }
}
