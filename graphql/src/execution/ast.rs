use std::ops::Deref;

use graph::prelude::{q, r};
use graphql_parser::Pos;

#[derive(Debug, Clone, PartialEq)]
pub struct FragmentDefinition {
    pub position: Pos,
    pub name: String,
    pub type_condition: TypeCondition,
    pub directives: Vec<Directive>,
    pub selection_set: SelectionSet,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SelectionSet {
    span: (Pos, Pos),
    items: Vec<Selection>,
}

impl SelectionSet {
    pub fn new(span: (Pos, Pos), items: Vec<Selection>) -> Self {
        SelectionSet { span, items }
    }

    pub fn empty_from(other: &SelectionSet) -> Self {
        SelectionSet {
            span: other.span.clone(),
            items: Vec::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn included(&self) -> impl Iterator<Item = &Selection> {
        self.items.iter().filter(|selection| selection.selected())
    }

    pub fn selections(&self) -> impl Iterator<Item = &Selection> {
        self.items.iter()
    }

    pub fn push(&mut self, field: Field) {
        self.items.push(Selection::Field(field))
    }
}

impl Extend<Selection> for SelectionSet {
    fn extend<T: IntoIterator<Item = Selection>>(&mut self, iter: T) {
        self.items.extend(iter)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Selection {
    Field(Field),
    FragmentSpread(FragmentSpread),
    InlineFragment(InlineFragment),
}

impl Selection {
    /// Looks up a directive in a selection, if it is provided.
    pub fn get_directive(&self, name: &str) -> Option<&Directive> {
        match self {
            Selection::Field(field) => field
                .directives
                .iter()
                .find(|directive| directive.name == name),
            _ => None,
        }
    }

    /// Returns true if a selection should be skipped (as per the `@skip` directive).
    fn skip(&self) -> bool {
        match self.get_directive("skip") {
            Some(directive) => match directive.argument_value("if") {
                Some(val) => match val {
                    // Skip if @skip(if: true)
                    r::Value::Boolean(skip_if) => *skip_if,
                    _ => false,
                },
                None => true,
            },
            None => false,
        }
    }

    /// Returns true if a selection should be included (as per the `@include` directive).
    fn include(&self) -> bool {
        match self.get_directive("include") {
            Some(directive) => match directive.argument_value("if") {
                Some(val) => match val {
                    // Include if @include(if: true)
                    r::Value::Boolean(include) => *include,
                    _ => false,
                },
                None => true,
            },
            None => true,
        }
    }

    fn selected(&self) -> bool {
        !self.skip() && self.include()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Directive {
    pub position: Pos,
    pub name: String,
    pub arguments: Vec<(String, r::Value)>,
}

impl Directive {
    /// Looks up the value of an argument in a vector of (name, value) tuples.
    pub fn argument_value(&self, name: &str) -> Option<&r::Value> {
        self.arguments
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, v)| v)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Field {
    pub position: Pos,
    pub alias: Option<String>,
    pub name: String,
    pub arguments: Vec<(String, r::Value)>,
    pub directives: Vec<Directive>,
    pub selection_set: SelectionSet,
}

impl Field {
    /// Returns the response key of a field, which is either its name or its alias (if there is one).
    pub fn response_key(&self) -> &str {
        self.alias
            .as_ref()
            .map(Deref::deref)
            .unwrap_or(self.name.as_str())
    }

    /// Looks up the value of an argument in a vector of (name, value) tuples.
    pub fn argument_value(&self, name: &str) -> Option<&r::Value> {
        self.arguments
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, v)| v)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct FragmentSpread {
    pub position: Pos,
    pub fragment_name: String,
    pub directives: Vec<Directive>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TypeCondition {
    On(String),
}

impl From<q::TypeCondition> for TypeCondition {
    fn from(type_cond: q::TypeCondition) -> Self {
        let q::TypeCondition::On(name) = type_cond;
        TypeCondition::On(name)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct InlineFragment {
    pub position: Pos,
    pub type_condition: Option<TypeCondition>,
    pub directives: Vec<Directive>,
    pub selection_set: SelectionSet,
}
