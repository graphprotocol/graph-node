use lazy_static::lazy_static;

use graph::prelude::*;

lazy_static! {
    pub static ref SCHEMA: Arc<Schema> = {
        let raw_schema = include_str!("./schema.graphql");
        let document = graphql_parser::parse_schema(&raw_schema).unwrap();
        let (interfaces_for_type, types_for_interface) =
            Schema::collect_interfaces(&document).unwrap();

        Arc::new(Schema {
            id: SubgraphDeploymentId::new("indexnode").unwrap(),
            document: document,
            interfaces_for_type,
            types_for_interface,
        })
    };
}
