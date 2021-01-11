use diesel::{dsl::sql, prelude::*};
use diesel::{sql_types::Text, PgConnection};

use graph::prelude::anyhow;
use graph_store_postgres::command_support::catalog as store_catalog;

use crate::manager::display::List;

#[derive(Queryable, PartialEq, Eq, Hash)]
pub struct Deployment {
    pub name: String,
    pub status: String,
    pub deployment: String,
    pub namespace: String,
    pub node_id: Option<String>,
    pub shard: String,
}

impl Deployment {
    pub fn lookup(conn: &PgConnection, name: String) -> Result<Vec<Self>, anyhow::Error> {
        use store_catalog::deployment_schemas as ds;
        use store_catalog::subgraph as s;
        use store_catalog::subgraph_deployment_assignment as a;
        use store_catalog::subgraph_version as v;

        let query = ds::table
            .inner_join(v::table.on(v::deployment.eq(ds::subgraph)))
            .inner_join(s::table.on(v::subgraph.eq(s::id)))
            .left_outer_join(a::table.on(a::id.eq(ds::subgraph)))
            .select((
                s::name,
                sql::<Text>(
                    "(case
                    when subgraphs.subgraph.pending_version = subgraphs.subgraph_version.id then 'pending'
                    when subgraphs.subgraph.current_version = subgraphs.subgraph_version.id then 'current'
                    else 'unused' end) status",
                ),
                v::deployment,
                ds::name,
                a::node_id.nullable(),
                ds::shard,
            ));

        let deployments: Vec<Deployment> = if name.starts_with("sgd") {
            query.filter(ds::name.eq(&name)).load(conn)?
        } else if name.starts_with("Qm") {
            query.filter(ds::subgraph.eq(&name)).load(conn)?
        } else {
            // A subgraph name
            let pattern = format!("%{}%", name);
            query.filter(s::name.ilike(&pattern)).load(conn)?
        };
        Ok(deployments)
    }

    pub fn print_table(deployments: Vec<Self>) {
        let mut list = List::new(vec![
            "name",
            "status",
            "id",
            "namespace",
            "shard",
            "node_id",
        ]);

        for deployment in deployments {
            list.append(vec![
                deployment.name,
                deployment.status,
                deployment.deployment,
                deployment.namespace,
                deployment.shard,
                deployment.node_id.unwrap_or("---".to_string()),
            ]);
        }

        list.render();
    }
}
