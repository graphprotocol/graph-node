use graphql_parser::schema;

/// A GraphQL schema with additional meta data.
#[derive(Clone, Debug)]
pub struct Schema {
    pub id: String,
    pub document: schema::Document,
}
