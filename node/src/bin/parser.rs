use std::{collections::HashSet, io, sync::Arc};

use graph::{
    anyhow::Result,
    blockchain::BlockchainKind,
    data::subgraph::UnresolvedSubgraphManifest,
    data_source::DataSourceTemplate,
    env::EnvVars,
    ipfs_client::IpfsClient,
    prelude::{serde_yaml, DeploymentHash, Link},
    slog::{self, o, Logger},
    tokio,
};
use graph_core::LinkResolver;

#[tokio::main]
async fn main() -> Result<()> {
    // let flags: Vec<String> = env::args().skip(1).collect();

    let stdin = io::stdin();

    loop {
        let mut ipfs_file = String::new();
        stdin.read_line(&mut ipfs_file)?;
        let ipfs_file = ipfs_file.trim_end_matches("\n").to_string();
        if ipfs_file.is_empty() {
            break;
        }

        let client = IpfsClient::new("https://api.thegraph.com/ipfs/").unwrap();
        let env_vars = Arc::new(EnvVars::default());
        let client: Arc<dyn graph::prelude::LinkResolver> =
            Arc::new(LinkResolver::new(vec![client], env_vars.clone()));
        let logger = Logger::root(slog::Discard, o!());

        println!("#### {}", ipfs_file);
        let mut static_events = HashSet::<String>::new();
        let mut template_events = HashSet::<String>::new();

        let content = client
            .cat(
                &logger,
                &Link {
                    link: ipfs_file.clone(),
                },
            )
            .await
            .unwrap();

        let raw: serde_yaml::Mapping = serde_yaml::from_slice(&content).unwrap();

        let kind = BlockchainKind::from_manifest(&raw).unwrap();
        dbg!(kind);

        let deployment = DeploymentHash::new(ipfs_file).unwrap();
        let manifest = UnresolvedSubgraphManifest::<graph_chain_ethereum::Chain>::parse(
            deployment.clone(),
            raw,
        )
        .unwrap();

        for ds in manifest.data_sources {
            let ds = ds.resolve(&client, &logger, 100).await.unwrap();

            let ds = match ds {
                graph::data_source::DataSource::Onchain(ds) => ds,
                graph::data_source::DataSource::Offchain(_) => continue,
            };

            for event in ds.mapping.event_handlers {
                static_events.insert(event.event);
            }
        }

        for (i, template) in manifest.templates.into_iter().enumerate() {
            let template: DataSourceTemplate<graph_chain_ethereum::Chain> =
                template.resolve(&client, &logger, i as u32).await.unwrap();

            let template = match template {
                graph::data_source::DataSourceTemplate::Onchain(t) => t,
                graph::data_source::DataSourceTemplate::Offchain(_) => continue,
            };

            for event in template.mapping.event_handlers {
                template_events.insert(event.event);
            }
        }

        println!("static events: {:#?}", static_events);
        println!("template events: {:#?}", template_events);
        println!(
            "common events: {:#?}",
            template_events.intersection(&static_events)
        );
        println!("#####################################################",);
    }

    Ok(())
}
