use std::{collections::HashSet, env, fs, time::Instant};

use graph::anyhow;
use graph_store_postgres::command_support::index::{CreateIndex, Expr};

/// Parse index definitions from a file and print information about any that
/// we could not parse.
///
/// The easiest way to create a file with index definitions is to run this
/// query in psql:
/// ```sql
/// select indexdef from pg_indexes where schemaname like 'sgd%' \g /tmp/idxs.txt
/// ```
pub fn main() -> anyhow::Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        return Err(anyhow::anyhow!("usage: create_index <index_file>"));
    }
    let idxs = fs::read_to_string(&args[1])?;

    let mut parsed: usize = 0;
    let mut failed: usize = 0;
    let mut skipped: usize = 0;
    let mut unknown_cols = HashSet::new();
    let start = Instant::now();
    for idxdef in idxs.lines() {
        let idxdef = idxdef.trim();
        if idxdef.is_empty() || !idxdef.starts_with("CREATE") && !idxdef.starts_with("create") {
            skipped += 1;
            continue;
        }

        let idx = CreateIndex::parse(idxdef.to_string());

        match &idx {
            CreateIndex::Parsed { columns, .. } => {
                let mut failed_col = false;
                for column in columns {
                    match column {
                        Expr::Unknown(expr) => {
                            unknown_cols.insert(expr.clone());
                            failed_col = true;
                            break;
                        }
                        _ => { /* ok  */ }
                    }
                }
                if failed_col {
                    failed += 1;
                } else {
                    parsed += 1
                }
            }
            CreateIndex::Unknown { defn } => {
                println!("Can not parse index definition: {}", defn);
                failed += 1;
            }
        }
    }

    if !unknown_cols.is_empty() {
        println!("Unknown columns:");
        for col in unknown_cols {
            println!("  {}", col);
        }
    }

    println!(
        "total: {}, parsed: {parsed}, failed: {failed}, skipped: {skipped}, elapsed: {}s",
        parsed + failed + skipped,
        start.elapsed().as_secs()
    );
    Ok(())
}
