extern crate clap;
extern crate graph_store_postgres;

use clap::App;
use std::fs;
use std::process::exit;

use graph::prelude::{Schema, SubgraphDeploymentId, PRIMARY_SHARD};
use graph_store_postgres::relational::{Catalog, Column, ColumnType, Layout};

pub fn usage(msg: &str) -> ! {
    println!("layout: {}", msg);
    println!("Try 'layout --help' for more information.");
    std::process::exit(1);
}

pub fn ensure<T, E: std::fmt::Display>(res: Result<T, E>, msg: &str) -> T {
    match res {
        Ok(ok) => ok,
        Err(err) => {
            eprintln!("{}:\n    {}", msg, err);
            exit(1)
        }
    }
}

fn print_migration(layout: &Layout) {
    for table in layout.tables.values() {
        print!("insert into\n  {}(", table.qualified_name);
        for column in &table.columns {
            print!("{}, ", column.name.as_str());
        }
        print!("block_range)\nselect ");
        for (i, column) in table.columns.iter().enumerate() {
            if i > 0 {
                print!("       ");
            }
            if column.is_list() {
                print!(
                    "array(select (x->>'data')::{} from jsonb_array_elements(data->'{}'->'data') x)",
                    column.column_type.sql_type(),
                    column.field
                );
            } else {
                if column.name.as_str() == "id" {
                    print!("id");
                } else if column.column_type == ColumnType::Bytes {
                    print!(
                        "decode(replace(data->'{}'->>'data','0x',''),'hex')",
                        column.field
                    );
                } else {
                    print!(
                        "(data->'{}'->>'data')::{}",
                        column.field,
                        column.column_type.sql_type()
                    );
                }
            }
            println!(",");
        }
        println!("      block_range");
        println!("  from history_data");
        println!(" where entity = '{}';\n", table.object);
    }
}

fn print_views(layout: &Layout) {
    for table in layout.tables.values() {
        println!("\ncreate view {} as", table.qualified_name);
        print!("select ");
        for (i, column) in table.columns.iter().enumerate() {
            if i > 0 {
                print!(",\n       ");
            }
            if column.is_list() {
                print!(
                    "array(select (x->>'data')::{} from jsonb_array_elements(data->'{}'->'data') x)",
                    column.column_type.sql_type(),
                    column.field
                );
            } else {
                if column.name.as_str() == "id" {
                    print!("id");
                } else {
                    print!(
                        "(data->'{}'->>'data')::{}",
                        column.field,
                        column.column_type.sql_type()
                    );
                }
            }
            print!(" as {}", column.name.as_str());
        }
        println!("\n  from {}.entities", layout.catalog.schema);
        println!(" where entity = '{}';", table.object);
    }
}

fn print_drop_views(layout: &Layout) {
    for table in layout.tables.values() {
        println!("drop view if exists {};", table.qualified_name);
    }
}

fn print_drop(layout: &Layout) {
    for table in layout.tables.values() {
        println!("drop table {};", table.qualified_name);
    }
}

fn print_delete_all(layout: &Layout) {
    for table in layout.tables.values() {
        println!("delete from {};", table.qualified_name);
    }
}

fn print_ddl(layout: &Layout) {
    let ddl = ensure(layout.as_ddl(), "Failed to generate DDL");
    println!("{}", ddl);
}

/// Generate a SQL statement that copies dynamic data sources and modifies
/// ids of the data sources and all related objects as it copies the data.
/// The query will expect three bind variables:
///   $1 : text[] - the ids of the dynamic data sources to copy
///   $2 : text[] - the new ids for each of the data sources in $1
///   $3 : text   - the id of the deployment to which the copies will belong
fn print_copy_dds(layout: &Layout) {
    // Generate a SQL snippet that translates the values in `column` into
    // the new ids using the xlat view we set up earlier in the query
    fn xlat(column: &str, is_list: bool) -> String {
        if is_list {
            // A poor man's map: turn the array into a table, transform
            // the array element and turn everything back into an array
            format!(
                "(select array_agg(x.new_id || right(a.elt, -40)) from unnest(e.{}) a(elt)) as {}",
                column, column
            )
        } else {
            format!("(x.new_id || right(e.{}, -40)) as {}", column, column)
        }
    }

    // The tables relevant for dynamic metadata
    let mut tables = layout
        .tables
        .values()
        .filter(|table| {
            !table.name.as_str().starts_with("subgraph")
                && table.name.as_str() != "dynamic_ethereum_contract_data_source"
        })
        .collect::<Vec<_>>();
    tables.sort_by_key(|table| table.name.as_str());

    // xlat translates old id's (passed in $1) into new id's (passed as $2)
    println!("with xlat as (");
    print!("  select * from unnest($1::text[], $2::text[]) as xlat(id, new_id))");
    for (i, table) in tables.iter().enumerate() {
        println!(",");
        println!(" md{} as (", i);
        print!("    insert into subgraphs.{}(", table.name);
        for column in table.columns.iter() {
            print!("{}, ", column.name);
        }
        println!("block_range)");
        print!("    select ");
        for column in table.columns.iter() {
            if column.is_primary_key() || column.is_reference() {
                print!("{}, ", xlat(column.name.as_str(), column.is_list()));
            } else {
                print!("{}, ", column.name);
            }
        }
        println!("block_range");
        print!(
            "      from subgraphs.{} e, xlat x\n     where left(e.id, 40) = x.id)",
            table.name
        );
    }
    println!(
        r#"
insert into subgraphs.dynamic_ethereum_contract_data_source(id, kind, name,
              network, source, mapping, templates, ethereum_block_hash,
              ethereum_block_number, deployment, block_range)
select x.new_id, e.kind, e.name, e.network, {source}, {mapping}, {templates},
       e.ethereum_block_hash, e.ethereum_block_number, $3 as deployment,
       e.block_range
  from xlat x, subgraphs.dynamic_ethereum_contract_data_source e
 where x.id = e.id"#,
        source = xlat("source", false),
        mapping = xlat("mapping", false),
        templates = xlat("templates", true),
    );
}

fn print_diesel_tables(layout: &Layout) {
    fn diesel_type(column: &Column) -> String {
        let mut dsl_type = match column.column_type {
            ColumnType::Boolean => "Bool",
            ColumnType::BigDecimal | ColumnType::BigInt => "Numeric",
            ColumnType::Bytes | ColumnType::BytesId => "Binary",
            ColumnType::Int => "Integer",
            ColumnType::String | ColumnType::Enum(_) | ColumnType::TSVector(_) => "Text",
        }
        .to_owned();

        if column.is_list() {
            dsl_type = format!("Array<{}>", dsl_type);
        }
        if column.is_nullable() {
            dsl_type = format!("Nullable<{}>", dsl_type);
        }
        dsl_type
    }

    fn rust_type(column: &Column) -> String {
        let mut dsl_type = match column.column_type {
            ColumnType::Boolean => "bool",
            ColumnType::BigDecimal | ColumnType::BigInt => "BigDecimal",
            ColumnType::Bytes | ColumnType::BytesId => "Vec<u8>",
            ColumnType::Int => "i32",
            ColumnType::String | ColumnType::Enum(_) | ColumnType::TSVector(_) => "String",
        }
        .to_owned();

        if column.is_list() {
            dsl_type = format!("Vec<{}>", dsl_type);
        }
        if column.is_nullable() {
            dsl_type = format!("Option<{}>", dsl_type);
        }
        dsl_type
    }

    let mut tables = layout.tables.values().collect::<Vec<_>>();
    tables.sort_by_key(|table| table.name.as_str());

    for table in &tables {
        println!("    table! {{");
        let name = table.qualified_name.as_str().replace("\"", "");
        println!("        {} (vid) {{", name);
        println!("            vid -> BigInt,");
        for column in &table.as_ref().columns {
            println!(
                "            {} -> {},",
                column.name.as_str(),
                diesel_type(column)
            );
        }
        println!("            block_range -> Range<Integer>,");
        println!("        }}");
        println!("    }}\n")
    }

    // Now generate Rust structs for all this
    for table in &tables {
        println!("    #[derive(Queryable, Clone, Debug)]");
        println!("    pub struct {} {{", table.object);
        println!("        pub vid: i64,");
        for column in &table.as_ref().columns {
            println!(
                "        pub {}: {},",
                column.name.as_str(),
                rust_type(column)
            );
        }
        println!("        pub block_range: (Bound<i32>, Bound<i32>),");
        println!("    }}\n")
    }
}

fn print_list_metadata(layout: &Layout) {
    let mut tables = layout
        .tables
        .values()
        .filter(|table| {
            table.name.as_str() != "subgraph" && table.name.as_str() != "subgraph_version"
        })
        .collect::<Vec<_>>();
    tables.sort_by_key(|table| table.name.as_str());

    println!("with dds as (");
    println!("  select *");
    println!("    from subgraphs.dynamic_ethereum_contract_data_source");
    println!("   where deployment = $1)");
    for (i, table) in tables.iter().enumerate() {
        if i > 0 {
            println!(" union all");
        }
        println!("select '{}' as entity, id", &table.object);
        let name = table.qualified_name.as_str().replace("\"", "");
        println!("  from {} e", name);
        if table.object == "DynamicEthereumContractDataSource" {
            println!(" where e.deployment = $1");
        } else {
            println!(" where left(e.id, 46) = $1");
            println!("    or left(e.id, 40) in (select id from dds)");
        }
    }
    println!(" order by entity, id");
}

pub fn main() {
    let args = App::new("layout")
    .version("1.0")
    .about("Information about the database schema for a GraphQL schema")
    .args_from_usage(
        "-g, --generate=[KIND] 'what kind of SQL to generate. Can be ddl (the default), migrate, delete, or drop'
        <schema>
        [db_schema]"
    )
    .get_matches();

    let schema = args.value_of("schema").unwrap();
    let db_schema = args.value_of("db_schema").unwrap_or("rel");
    let kind = args.value_of("generate").unwrap_or("ddl");

    let subgraph = SubgraphDeploymentId::new("Qmasubgraph").unwrap();
    let schema = ensure(fs::read_to_string(schema), "Can not read schema file");
    let schema = ensure(Schema::parse(&schema, subgraph), "Failed to parse schema");
    let catalog = ensure(
        Catalog::make_empty(db_schema.to_owned()),
        "Failed to construct catalog",
    );
    let shard = PRIMARY_SHARD.to_string();
    let layout = ensure(
        Layout::new(shard, &schema, catalog, false),
        "Failed to construct Mapping",
    );
    match kind {
        "migrate" => {
            print_drop_views(&layout);
            print_ddl(&layout);
            print_migration(&layout);
        }
        "drop" => print_drop(&layout),
        "delete" => print_delete_all(&layout),
        "ddl" => print_ddl(&layout),
        "views" => print_views(&layout),
        "drop-views" => print_drop_views(&layout),
        "diesel" => print_diesel_tables(&layout),
        "list-metadata" => print_list_metadata(&layout),
        "copy-dds" => print_copy_dds(&layout),
        _ => {
            usage(&format!("illegal value {} for --generate", kind));
        }
    }
}
