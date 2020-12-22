use std::collections::BTreeMap;

use diesel::PgConnection;
use diesel::{dsl::any, prelude::*};

use graph::prelude::anyhow;
use graph_store_postgres::command_support::catalog as store_catalog;

#[derive(PartialEq, Eq, Hash)]
pub struct Deployment {
    pub id: String,
    pub namespace: String,
    pub name: Option<String>,
    pub shard: String,
}

impl Deployment {
    pub fn lookup(conn: &PgConnection, name: String) -> Result<Vec<Self>, anyhow::Error> {
        use store_catalog::deployment_schemas as ds;
        use store_catalog::subgraph as s;
        use store_catalog::subgraph_version as v;

        let ids = if name.starts_with("sgd") {
            ds::table
                .filter(ds::name.eq(&name))
                .select(ds::subgraph)
                .load::<String>(conn)?
        } else if name.starts_with("Qm") {
            ds::table
                .filter(ds::subgraph.eq(&name))
                .select(ds::subgraph)
                .load(conn)?
        } else {
            // A subgraph name
            let pattern = format!("%{}%", name);
            v::table
                .inner_join(s::table.on(s::current_version.eq(v::id.nullable())))
                .filter(s::name.ilike(&pattern))
                .select(v::deployment)
                .load(conn)?
        };
        Self::deployments(conn, ids)
    }

    fn deployments(conn: &PgConnection, ids: Vec<String>) -> Result<Vec<Self>, anyhow::Error> {
        use store_catalog::deployment_schemas as ds;
        use store_catalog::subgraph as s;
        use store_catalog::subgraph_version as v;

        let deployments = s::table
            .inner_join(v::table.on(s::id.eq(v::subgraph)))
            .inner_join(ds::table.on(v::deployment.eq(ds::subgraph)))
            .filter(v::deployment.eq(any(ids)))
            .order_by(s::name)
            .select((v::deployment, ds::name, s::name, ds::shard))
            .distinct()
            .load(conn)?
            .into_iter()
            .map(|(id, namespace, name, shard)| Deployment {
                id,
                namespace,
                name: Some(name),
                shard,
            })
            .collect::<Vec<_>>();
        Ok(deployments)
    }

    pub fn print_table(deployments: Vec<Self>) {
        let mut first = true;
        for deployment in deployments {
            if !first {
                println!("-------------+-----------------------------------------------");
            }
            first = false;

            println!(
                "{:12} | {}",
                "name",
                deployment.name.unwrap_or("---".to_string())
            );
            println!("{:12} | {}", "id", deployment.id);
            println!("{:12} | {}", "nsp", deployment.namespace);
            println!("{:12} | {}", "shard", deployment.shard);
        }
    }

    pub fn print_by_deployment(deployments: Vec<Self>) {
        let mut by_id: BTreeMap<String, Vec<Deployment>> = BTreeMap::new();
        for deployment in deployments {
            by_id
                .entry(deployment.id.clone())
                .or_default()
                .push(deployment);
        }

        let mut first = true;
        for deployments in by_id.values().into_iter() {
            if !first {
                println!("-------------+-----------------------------------------------");
            }
            first = false;

            let deployment = deployments.first().unwrap();
            println!("{:12} | {}", "id", deployment.id);
            println!("{:12} | {}", "nsp", deployment.namespace);
            println!("{:12} | {}", "shard", deployment.shard);

            let mut name = "name";
            for deployment in deployments {
                println!(
                    "{:12} | {}",
                    name,
                    deployment.name.as_deref().unwrap_or("---")
                );
                name = "";
            }
        }
    }
}
