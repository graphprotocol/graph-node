mod constants;
mod parser;
mod validation;

pub use parser::Parser;

#[cfg(test)]
mod test {
    use std::{collections::BTreeSet, sync::Arc};

    use graph::{prelude::DeploymentHash, schema::InputSchema};

    use crate::{
        catalog::Catalog,
        primary::{make_dummy_site, Namespace},
        relational::Layout,
    };

    pub(crate) fn make_layout(gql: &str) -> Layout {
        let subgraph = DeploymentHash::new("Qmasubgraph").unwrap();
        let schema = InputSchema::parse_latest(gql, subgraph.clone()).unwrap();
        let namespace = Namespace::new("sgd0815".to_string()).unwrap();
        let site = Arc::new(make_dummy_site(subgraph, namespace, "anet".to_string()));
        let catalog = Catalog::for_tests(site.clone(), BTreeSet::new()).unwrap();
        let layout = Layout::new(site, &schema, catalog).unwrap();
        layout
    }
}
