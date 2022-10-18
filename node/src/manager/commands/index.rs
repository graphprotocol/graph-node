use crate::manager::{color::Terminal, deployment::DeploymentSearch, CmdResult};
use graph::{
    components::store::DeploymentLocator,
    itertools::Itertools,
    prelude::{anyhow, StoreError},
};
use graph_store_postgres::{
    command_support::index::{CreateIndex, Method},
    connection_pool::ConnectionPool,
    SubgraphStore,
};
use std::io::Write as _;
use std::{collections::HashSet, sync::Arc};

fn validate_fields<T: AsRef<str>>(fields: &[T]) -> Result<(), anyhow::Error> {
    // Must be non-empty. Double checking, since [`StructOpt`] already checks this.
    if fields.is_empty() {
        anyhow::bail!("at least one field must be informed")
    }
    // All values must be unique
    let unique: HashSet<_> = fields.iter().map(AsRef::as_ref).collect();
    if fields.len() != unique.len() {
        anyhow::bail!("entity fields must be unique")
    }
    Ok(())
}
pub async fn create(
    store: Arc<SubgraphStore>,
    pool: ConnectionPool,
    search: DeploymentSearch,
    entity_name: &str,
    field_names: Vec<String>,
    index_method: String,
) -> Result<(), anyhow::Error> {
    validate_fields(&field_names)?;
    let deployment_locator = search.locate_unique(&pool)?;
    println!("Index creation started. Please wait.");
    let index_method = Method::try_from(index_method.as_str())
        .map_err(|_| anyhow!("unknown index method `{}`", index_method))?;
    match store
        .create_manual_index(&deployment_locator, entity_name, field_names, index_method)
        .await
    {
        Ok(()) => Ok(()),
        Err(StoreError::Canceled) => {
            eprintln!("Index creation attempt faield. Please retry.");
            ::std::process::exit(1);
        }
        Err(other) => Err(anyhow::anyhow!(other)),
    }
}

pub async fn list(
    store: Arc<SubgraphStore>,
    pool: ConnectionPool,
    search: DeploymentSearch,
    entity_name: &str,
    no_attribute_indexes: bool,
    no_default_indexes: bool,
) -> Result<(), anyhow::Error> {
    fn header(
        term: &mut Terminal,
        indexes: &[CreateIndex],
        loc: &DeploymentLocator,
        entity: &str,
    ) -> Result<(), anyhow::Error> {
        use CreateIndex::*;

        let index = indexes.iter().find(|index| matches!(index, Parsed { .. }));
        match index {
            Some(Parsed { nsp, table, .. }) => {
                term.bold()?;
                writeln!(term, "{:^76}", format!("Indexes for {nsp}.{table}"))?;
                term.reset()?;
            }
            _ => {
                writeln!(
                    term,
                    "{:^76}",
                    format!("Indexes for sgd{}.{entity}", loc.id)
                )?;
            }
        }
        writeln!(term, "{: ^12} IPFS hash: {}", "", loc.hash)?;
        writeln!(term, "{:-^76}", "")?;
        Ok(())
    }

    fn footer(term: &mut Terminal) -> Result<(), anyhow::Error> {
        writeln!(term, "  (a): account-like flag set")?;
        Ok(())
    }

    fn print_index(term: &mut Terminal, index: &CreateIndex) -> CmdResult {
        use CreateIndex::*;

        match index {
            Unknown { defn } => {
                writeln!(term, "*unknown*")?;
                writeln!(term, "  {defn}")?;
            }
            Parsed {
                unique,
                name,
                nsp: _,
                table: _,
                method,
                columns,
                cond,
                with,
            } => {
                let unique = if *unique { " unique" } else { "" };
                let start = format!("{unique} using {method}");
                let columns = columns.into_iter().map(|c| c.to_string()).join(", ");

                term.green()?;
                if index.is_default_index() {
                    term.dim()?;
                } else {
                    term.bold()?;
                }
                write!(term, "{name}")?;
                term.reset()?;
                write!(term, "{start}")?;
                term.blue()?;
                if name.len() + start.len() + columns.len() <= 76 {
                    writeln!(term, "({columns})")?;
                } else {
                    writeln!(term, "\n  on ({})", columns)?;
                }
                term.reset()?;
                if let Some(cond) = cond {
                    writeln!(term, "  where {cond}")?;
                }
                if let Some(with) = with {
                    writeln!(term, "  with {with}")?;
                }
            }
        }
        Ok(())
    }

    let deployment_locator = search.locate_unique(&pool)?;
    let indexes: Vec<_> = {
        let mut indexes = store
            .indexes_for_entity(&deployment_locator, entity_name)
            .await?;
        if no_attribute_indexes {
            indexes = indexes
                .into_iter()
                .filter(|idx| !idx.is_attribute_index())
                .collect();
        }
        if no_default_indexes {
            indexes = indexes
                .into_iter()
                .filter(|idx| !idx.is_default_index())
                .collect();
        }
        indexes
    };

    let mut term = Terminal::new();

    let mut first = true;
    header(&mut term, &indexes, &deployment_locator, entity_name)?;
    for index in &indexes {
        if !first {
            writeln!(term, "{:-^76}", "")?;
            first = false;
        }
        print_index(&mut term, index)?;
    }
    Ok(())
}

pub async fn drop(
    store: Arc<SubgraphStore>,
    pool: ConnectionPool,
    search: DeploymentSearch,
    index_name: &str,
) -> Result<(), anyhow::Error> {
    let deployment_locator = search.locate_unique(&pool)?;
    store
        .drop_index_for_deployment(&deployment_locator, &index_name)
        .await?;
    println!("Dropped index {index_name}");
    Ok(())
}
