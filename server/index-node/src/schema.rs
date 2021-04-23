use graph::prelude::*;

lazy_static! {
    pub static ref SCHEMA: Arc<ApiSchema> = {
        let raw_schema = include_str!("./schema.graphql");
        let document = graphql_parser::parse_schema(&raw_schema).unwrap();
        let (interfaces_for_type, types_for_interface) =
            Schema::collect_interfaces(&document).unwrap();

        Arc::new(
            ApiSchema::from_api_schema(Schema {
                id: DeploymentHash::new("indexnode").unwrap(),
                document,
                interfaces_for_type,
                types_for_interface,
            })
            .unwrap(),
        )
    };
}

#[test]
fn schema_parses() {
    &*SCHEMA;
}
