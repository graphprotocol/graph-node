extern crate clap;
extern crate graph_store_postgres;

use clap::App;
use graphql_parser::parse_schema;
use std::fs;
use std::process::exit;

use graph::prelude::SubgraphDeploymentId;
use graph_store_postgres::relational::{ColumnType, IdType, Layout};

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
        println!("\n  from {}.entities", layout.schema);
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

    let schema = ensure(fs::read_to_string(schema), "Can not read schema file");
    let schema = ensure(parse_schema(&schema), "Failed to parse schema");
    let subgraph = SubgraphDeploymentId::new("Qmasubgraph").unwrap();
    let layout = ensure(
        Layout::new(&schema, IdType::String, subgraph, db_schema),
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
        _ => {
            usage(&format!("illegal value {} for --generate", kind));
        }
    }
}
