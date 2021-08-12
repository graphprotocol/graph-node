use std::io::prelude::*;
use std::io::{BufWriter, Seek, Write};
use std::iter::Iterator;
use std::path::Path;
use walkdir::{DirEntry, WalkDir};
use zip::result::ZipError;
use zip::write::FileOptions;

use diesel::r2d2::{ConnectionManager, PooledConnection};
use diesel::sql_types::{BigInt, Integer, Text};
use diesel::PgConnection;
use diesel::{sql_query, RunQueryDsl};
use postgres::{Client as PGClient, NoTls, SimpleQueryMessage};

use graph::data::subgraph::DeploymentHash;
use graph::prelude::{anyhow, SubgraphName};
use graph_store_postgres::command_support::catalog as store_catalog;
use graph_store_postgres::connection_pool::ConnectionPool;

use serde::Deserialize;
use std::fs::{self, File};

//Nothing here requires async
use reqwest::blocking::multipart;
use reqwest::blocking::Client;

/*
Process:
0. Dump your poi table and post it to a remote store with metadata pertinent to indexer.
1. Poll external api for divergent block.
2. Use divergent block to filter for entity updates, external data sources, and the ethereum call cache.
3. Post this data back to dispute service.
4. Wait for arbitration.
*/

//Modify this to run sql query that dumps database of poi to a file. Return the file location.

//Create a directory for local dumps if doesn't exist. Make the dump database contain a directory with the deployment id.

//@TODO: Allow for a subgraph name to be used for dumping. Allow for potential disputes in the indexer to be used too.

pub fn run(
    pool: ConnectionPool,
    dispute_id: String,
    indexer_id: String,
    deployment_id: String,
    subgraph_name: Option<String>,
) -> Result<(), anyhow::Error> {
    let foreign_server = pool.connection_detail()?;
    let synchronous_client = PGClient::connect(
        &format!(
            "host={} user={} password={} port={} dbname={}",
            foreign_server.host,
            foreign_server.user,
            foreign_server.password,
            foreign_server.port,
            foreign_server.dbname
        ),
        NoTls,
    );
    let pooled_connection = pool.get()?;

    let connection = store_catalog::Connection::new(&pooled_connection);

    let deployment_hash = match subgraph_name {
        Some(name) => {
            connection.current_deployment_for_subgraph(SubgraphName::new(name).unwrap())?
        }
        None => DeploymentHash::new(deployment_id).unwrap(),
    };

    let deployment_hash_string = deployment_hash.as_str();
    let subgraph_schema_name =
        get_subgraph_schema_name(&pooled_connection, &deployment_hash_string)?;

    println!("creating dump directory {}", &deployment_hash_string);
    let directory = create_dump_directory(deployment_hash_string)?;
    let poi_records = get_poi(&pooled_connection, &subgraph_schema_name)?;
    let tmp_filepath = dump_poi(&directory, poi_records)?;
    walk_and_compress(&tmp_filepath, &tmp_filepath)?;
    upload_file_to_endpoint(&tmp_filepath, indexer_id, dispute_id)?;
    delete_directory(&tmp_filepath)?;

    Ok(())
}

/// Create a directory to store files for upload
fn create_dump_directory(deployment_id: &str) -> Result<String, anyhow::Error> {
    let path = format!("/tmp/graph_node/data_dump/{}", deployment_id);
    fs::create_dir_all(&path)?;
    Ok(path)
}

/// Walks a directory path for all files and adds them to compressed archive.
fn walk_and_compress(src_dir: &str, dst_file: &str) -> zip::result::ZipResult<()> {
    if !Path::new(src_dir).is_dir() {
        return Err(ZipError::FileNotFound);
    }

    let path = Path::new(dst_file);
    let file = File::create(&path).unwrap();

    let walkdir = WalkDir::new(src_dir.to_string());
    let it = walkdir.into_iter();

    compress(&mut it.filter_map(|e| e.ok()), src_dir, file)?;

    Ok(())
}

/// Compress contents of a directory
fn compress<T>(
    it: &mut dyn Iterator<Item = DirEntry>,
    prefix: &str,
    writer: T,
) -> zip::result::ZipResult<()>
where
    T: Write + Seek,
{
    let mut zip = zip::ZipWriter::new(writer);
    let options = FileOptions::default()
        .compression_method(zip::CompressionMethod::Deflated)
        .unix_permissions(0o755);

    let mut buffer = Vec::new();
    for entry in it {
        let path = entry.path();
        let name = path.strip_prefix(Path::new(prefix)).unwrap();

        // Write file or directory explicitly
        // Some unzip tools unzip files with directory paths correctly, some do not!
        if path.is_file() {
            println!("Compressing file {:?} as {:?} ...", path, name);
            #[allow(deprecated)]
            zip.start_file_from_path(name, options)?;
            let mut f = File::open(path)?;

            f.read_to_end(&mut buffer)?;
            zip.write_all(&*buffer)?;
            buffer.clear();
        } else if name.as_os_str().len() != 0 {
            // Only if not root! Avoids path spec / warning
            // and mapname conversion failed error on unzip
            println!("Compressing directory {:?} as {:?} ...", path, name);
            #[allow(deprecated)]
            zip.add_directory_from_path(name, options)?;
        }
    }
    zip.finish()?;
    Result::Ok(())
}

/// Remove file after its been uploaded
fn delete_directory(directory: &str) -> Result<(), anyhow::Error> {
    fs::remove_dir_all(directory)?;
    Ok(())
}

#[derive(Queryable, QueryableByName)]
pub struct PoiRecord {
    #[sql_type = "Text"]
    digest: String,
    #[sql_type = "Text"]
    id: String,
    #[sql_type = "BigInt"]
    vid: i64,
    #[sql_type = "Text"]
    block_range: String,
}

/// Gather a set of poi query records
pub fn get_poi(
    conn: &PooledConnection<ConnectionManager<PgConnection>>,
    subgraph_schema_name: &str,
) -> Result<Vec<PoiRecord>, anyhow::Error> {
    let raw_query = format!(
        "SELECT encode(poi2$.digest,'hex') as digest, id,vid, CAST(block_range as text) from {}.poi2$",
        subgraph_schema_name
    );

    let poi_queryset = sql_query(raw_query).load::<PoiRecord>(conn).unwrap();
    return Ok(poi_queryset);
}

#[derive(Queryable, QueryableByName)]
struct SubgraphNameRecord {
    #[sql_type = "Text"]
    subgraph_name: String,
}

/// Schema is required for querying entities and POI
fn get_subgraph_schema_name(
    conn: &PooledConnection<ConnectionManager<PgConnection>>,
    deployment_id: &str,
) -> Result<String, anyhow::Error> {
    let raw_query = format!(
        "SELECT name from deployment_schemas WHERE subgraph = {} LIMIT 1",
        deployment_id
    );
    let name_result = sql_query(raw_query).get_result::<SubgraphNameRecord>(conn)?;

    Ok(name_result.subgraph_name)
}

#[derive(Queryable, QueryableByName)]
struct ChainNameSpaceRecord {
    #[sql_type = "Text"]
    namespace: String,
}

/// Chain namespace is required for querying call cache
fn get_subgraph_network_chain(
    conn: &PooledConnection<ConnectionManager<PgConnection>>,
    deployment_id: &str,
) -> Result<String, anyhow::Error> {
    let raw_query = format!(
        "SELECT namespace from chains where name = (SELECT network from deployment_schemas WHERE subgraph = {} LIMIT 1)",
        deployment_id
    );
    let name_result = sql_query(raw_query).get_result::<ChainNameSpaceRecord>(conn)?;

    Ok(name_result.namespace)
}

fn dump_poi(
    directory: &str,
    poi_records: std::vec::Vec<PoiRecord>,
) -> Result<String, anyhow::Error> {
    println!("dumping poi");
    let filepath = format!("{}/poi/poi.csv", directory);
    let f = File::create(&filepath).expect("Unable to create file");
    let mut file_buffer = BufWriter::new(f);
    write!(file_buffer, "digest, id, vid, block_range\n").expect("Unable to write header");
    for poi in &poi_records {
        write!(
            file_buffer,
            "{},{},{},{}\n",
            poi.digest, poi.id, poi.vid, poi.block_range
        )
        .expect("unable to write row");
    }
    return Ok(filepath);
}

/// ## Takes a filepath and sends to the dispute service for persistence.
/// ### @TODO: Upon completion delete the file?
pub fn upload_file_to_endpoint(
    fp: &str,
    indexer_id: String,
    dispute_id: String,
) -> Result<(), anyhow::Error> {
    let form = multipart::Form::new().file("file", fp)?;
    let client = Client::new();
    let res = client
        .post("http://localhost:8000/upload")
        .header("Indexer-node", indexer_id)
        .header("Dispute-hash", dispute_id)
        .multipart(form)
        .send()?;

    if res.status().is_success() {
        println!("success!");
    } else if res.status().is_server_error() {
        println!("server error! {:?}", res.status());
    } else {
        println!("Something else happened. Status: {:?}", res.status());
    }

    Ok(())
}

pub fn get_divergent_blocks(
    dispute_id: String,
    indexer_id: String,
) -> Result<Vec<i32>, anyhow::Error> {
    #[derive(Deserialize)]
    struct DivergentBlockResponse {
        indexer_id: String,
        divergent_blocks: Vec<i32>,
    }

    let client = Client::new();
    let res = client
        .get("http://localhost:8000/divergent_blocks")
        .header("Indexer-node", indexer_id)
        .header("Dispute-hash", dispute_id)
        .send()?;

    if res.status().is_success() {
        println!("success!");
    } else if res.status().is_server_error() {
        println!("server error! {:?}", res.status());
    } else {
        println!("Something else happened. Status: {:?}", res.status());
    }

    let json: DivergentBlockResponse = res.json()?;
    //get result as json and parse out divergent_blocks

    return Ok(json.divergent_blocks);
}

#[derive(Queryable, QueryableByName)]
pub struct CallCacheRecord {
    #[sql_type = "Text"]
    id: String,
    #[sql_type = "Text"]
    return_value: String,
    #[sql_type = "Text"]
    contract_address: String,
    #[sql_type = "Integer"]
    block_number: i32,
}

pub fn get_call_cache(
    conn: &PooledConnection<ConnectionManager<PgConnection>>,
    chain_namespace: &str,
    divergent_block: i32,
) -> Result<Vec<CallCacheRecord>, anyhow::Error> {
    let raw_query = format!(
        "SELECT encode(id,'hex') as id, encode(return_value,'hex') as \
        return_value,encode(contract_address,'hex') as contract_address,\
        block_number from {}.call_cache WHERE block_number = {}",
        chain_namespace, divergent_block
    );

    let callcache_queryset = sql_query(raw_query).load::<CallCacheRecord>(conn).unwrap();
    return Ok(callcache_queryset);
}

fn dump_call_cache(
    directory: &str,
    callcache_records: std::vec::Vec<CallCacheRecord>,
) -> Result<String, anyhow::Error> {
    println!("dumping call cache");
    let filepath = format!("{}/call_cache/call_cache.csv", directory);
    let f = File::create(&filepath).expect("Unable to create file");
    let mut file_buffer = BufWriter::new(f);
    write!(
        file_buffer,
        "id, return_value, contract_address, block_number\n"
    )
    .expect("Unable to write header");
    for cc in &callcache_records {
        write!(
            file_buffer,
            "{},{},{},{}\n",
            cc.id, cc.return_value, cc.contract_address, cc.block_number
        )
        .expect("unable to write row");
    }
    return Ok(filepath);
}

#[derive(Queryable, QueryableByName)]
pub struct TableName {
    #[sql_type = "Text"]
    tablename: String,
}

/// Get all tables present in the subgraph schema. Will be used for dumping data
fn get_subgraph_tables(
    conn: &PooledConnection<ConnectionManager<PgConnection>>,
    subgraph_schema_name: &str,
) -> Result<Vec<TableName>, anyhow::Error> {
    let query = format!(
        "select tablename from pg_tables where schemaname='{}'",
        subgraph_schema_name
    );

    let subgraph_tables = sql_query(query).get_results::<TableName>(conn).unwrap();
    return Ok(subgraph_tables);
}

/// Used to persist String
fn string_to_static_str(s: String) -> &'static str {
    Box::leak(s.into_boxed_str())
}

#[derive(Queryable, QueryableByName)]
pub struct ColumnNameAndType {
    #[sql_type = "Text"]
    column_name: String,
    #[sql_type = "Text"]
    data_type: String,
}

/// Gathers entries from entity tables that contain the divergent block if in closed interval
/// Diesel isn't of any use here because this is dynamic.
/// Also need to catch the columns for passing to csv.
/// Don't Run this on the poi2$ table
fn get_filtered_subgraph_table_rows(
    conn: &PooledConnection<ConnectionManager<PgConnection>>,
    raw_client: &mut PGClient,
    table_name: String,
    schema_name: String,
    divergent_block: i32,
) -> Result<(Vec<ColumnNameAndType>, Vec<Vec<String>>), anyhow::Error> {
    // We won't know the type of the returned table rows at compile time.
    // And you can't use a placeholder for a table name when substituting a query string.
    // Need to format prior and use the raw postgres driver.

    let column_query = format!(
        "SELECT column_name,data_type FROM information_schema.columns WHERE table_schema = '{}'
     AND table_name   = '{}'
       ORDER by ordinal_position",
        schema_name, table_name
    );
    // let columns = raw_client.query(column_query, &[&schema_name, &table_name])?;

    let columns = sql_query(column_query)
        .get_results::<ColumnNameAndType>(conn)
        .unwrap();

    let entity_query = format!("select * from {}.{} as {} WHERE ({}::int4  <@ {}.block_range AND upper_inc({}.block_range))\
OR ({}::int4 = lower({}.block_range))",schema_name, table_name, table_name,divergent_block, table_name, table_name,divergent_block,table_name);

    // Hack to get around the table name formatting
    // let static_entity_query: &'static str = string_to_static_str(entity_query);

    // Returns rows as strings
    // Issue with `db error: ERROR: prepared statement "s0" already exists` when not using simple_query

    let query_vec = raw_client.simple_query(&entity_query)?;

    let mut row_entries: Vec<Vec<String>> = vec![];

    for query_message in &query_vec {
        let mut single_row_vector: Vec<String> = vec![];
        match query_message {
            SimpleQueryMessage::Row(query_row) => {
                for idx in 0..query_row.len() {
                    let item = query_row.get(idx).unwrap().to_string();
                    single_row_vector.push(item);
                }
                row_entries.push(single_row_vector);
            }
            _ => println!("Empty result"),
        }
    }

    Ok((columns, row_entries))
}

fn dump_divergent_entities(
    directory: &str,
    table_columns: Vec<ColumnNameAndType>,
    table_records: Vec<Vec<String>>,
    table_name: String,
) -> Result<String, anyhow::Error> {
    println!("dumping divergent entities");

    let record_filepath = format!("{}/{}_entities.csv", directory, table_name);
    let record_file = File::create(&record_filepath).expect("Unable to create record file");

    let meta_filepath = format!("{}/{}_entities_metadata.csv", directory, table_name);
    let meta_file = File::create(&meta_filepath).expect("Unable to create metdata file");

    let mut record_buffer = BufWriter::new(record_file);
    let mut metadata_buffer = BufWriter::new(meta_file);

    write!(metadata_buffer, "column_name,data_type\n").expect("Unable to write metadata header");

    let mut record_header = vec![];

    for column_data in &table_columns {
        record_header.push(column_data.column_name.clone());
        let column_metadat_str = format!("{},{}\n", column_data.column_name, column_data.data_type);
        write!(metadata_buffer, "{}", column_metadat_str).expect("Unable to write metadata row");
    }

    let record_header_str = record_header.join(",");

    write!(record_buffer, "{}\n", record_header_str).expect("Unable to write record header");

    for record in &table_records {
        write!(record_buffer, "{}\n", record.join(",")).expect("unable to write record row");
    }

    return Ok(record_filepath);
}
