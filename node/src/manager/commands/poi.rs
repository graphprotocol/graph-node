use std::io::prelude::*;
use std::io::{Seek, Write};
use std::iter::Iterator;
use std::path::{Path, PathBuf};
use walkdir::{DirEntry, WalkDir};
use zip::result::ZipError;
use zip::write::FileOptions;

use diesel::r2d2::{ConnectionManager, PooledConnection};
use diesel::sql_types::{BigInt, Integer, Text};
use diesel::PgConnection;
use diesel::{sql_query, RunQueryDsl};
use postgres::{Client as PGClient, NoTls, SimpleQueryMessage};

use graph::data::subgraph::DeploymentHash;
use graph::prelude::anyhow::bail;
use graph::prelude::{anyhow, SubgraphName};
use graph_store_postgres::command_support::catalog as store_catalog;
use graph_store_postgres::connection_pool::ConnectionPool;

use serde::Deserialize;
use std::fs::{self, File};

//Nothing here requires async.
use reqwest::blocking::multipart;
use reqwest::blocking::Client;

use csv::WriterBuilder;

/// Provide a go-like defer statement
/// https://stackoverflow.com/questions/29963449/golang-like-defer-in-rust.
/// Just a souped up closure that calls on function exit.
struct ScopeCall<F: FnMut()> {
    c: F,
}
impl<F: FnMut()> Drop for ScopeCall<F> {
    fn drop(&mut self) {
        (self.c)();
    }
}

macro_rules! defer {
    ($e:expr) => {
        let _scope_call = ScopeCall {
            c: || -> () {
                $e;
            },
        };
    };
}

/*
Process:
0. Dump your poi table to a local file.
1. Compress the file .
2. Post the POI file to a remote store with metadata pertinent to indexer.
3. Poll external api for divergent block.
4. Use divergent block to filter for entity updates, external data sources, and the ethereum call cache.
5. Serialize the tables to csvs. Also capture the column data types for post processing.
6. Post the entity data back to dispute service.
7. Wait for arbitration.

@TODO: Allow for a subgraph name to be used for dumping.
@TODO: Make upload async so status can be displayed in a progress bar.
@TODO: Use tokio-postgres client instead of the synchronous one for entity filtering.
@TODO: Sign indexer's identity.
*/

/// Grabs `poi`, saves to intermittent file, compresses, attempts to upload, and finally cleans up.
///
/// @TODO: Shortcircuit failure. Expose a `GET` request on the dispute service to determine if indexer
/// should even attempt this process.
///
/// @TODO:Make use of async reqwests client.
///
/// @TODO:Make useful Error messages
pub fn sync_poi(
    pool: ConnectionPool,
    dispute_id: String,
    indexer_id: String,
    deployment_id: String,
    subgraph_name: Option<String>,
    debug: bool,
    host: String,
) -> Result<(), anyhow::Error> {
    let pooled_connection = pool.get()?;
    let connection = store_catalog::Connection::new(&pooled_connection);

    let deployment_hash = match subgraph_name {
        Some(name) => {
            connection.current_deployment_for_subgraph(SubgraphName::new(name).unwrap())?
        }
        None => DeploymentHash::new(&deployment_id).unwrap(),
    };

    let deployment_hash_string = deployment_hash.as_str();
    let subgraph_schema_name =
        get_subgraph_schema_name(&pooled_connection, &deployment_hash_string)?;

    println!("{}", &subgraph_schema_name);
    println!("Creating poi dump directory {}", &deployment_hash_string);

    let root_path = Path::new("/tmp/graph_node/data_dump");
    if !root_path.exists() {
        fs::create_dir_all(&root_path)?;
    }
    let base_path = root_path.join(&deployment_id);
    if !base_path.exists() {
        fs::create_dir_all(&base_path)?;
    }

    let base_copy = base_path.clone();
    //Register post-run cleanup
    defer!(delete_directory(&base_copy, debug).unwrap());
    create_dump_directories(&base_path, "poi".to_string())?;
    let poi_records = get_poi(&pooled_connection, &subgraph_schema_name)?;

    let src_directory = base_path.join("poi/raw");
    dump_poi_csv_writer(&src_directory, poi_records)?;
    let dst_directory = base_path.join("poi/compressed/poi_compressed.zip");

    walk_and_compress(&src_directory, &dst_directory)?;
    upload_file_to_endpoint(&dst_directory, &host, indexer_id, dispute_id, "poi")?;

    Ok(())
}

/// Grabs divergent blocks for the indexer. If they exist, proceeds to use them to filter entity tables
/// in the subgraph schema and writes them to `csvs`. Does the same to the `call_cache`. Also persists
/// data relevant to the types of records so that the strings can be recast into a format that
/// respects their original types. Compresses and uploads everything and then cleans up.
///
/// Part of the process requires invoking dynamic sql queries that depend on subgraph specific tables
/// That cannot be known at compile time. In this scenario the ORM is tossed out and the raw postgres
/// driver is used.
///
/// @TODO:Make use of the async postgres driver.
///
/// @TODO:Make use of async reqwests library.
///
/// @TODO:Make useful Error messages

pub fn sync_entities(
    pool: ConnectionPool,
    dispute_id: String,
    indexer_id: String,
    deployment_id: String,
    subgraph_name: Option<String>,
    debug: bool,
    host: String,
) -> Result<(), anyhow::Error> {
    //Short circuit if you error out on getting the divergent blocks
    let divergent_blocks = get_divergent_blocks(&host, &dispute_id, &indexer_id)?;
    let first_divergent_block = *divergent_blocks.first().unwrap();
    println! {"FIRST DIVERGENT BLOCK {}",first_divergent_block};
    let foreign_server = pool.connection_detail()?;
    let pooled_connection = pool.get()?;

    let connection = store_catalog::Connection::new(&pooled_connection);

    let deployment_hash = match subgraph_name {
        Some(name) => {
            connection.current_deployment_for_subgraph(SubgraphName::new(name).unwrap())?
        }
        None => DeploymentHash::new(&deployment_id).unwrap(),
    };

    let deployment_hash_string = deployment_hash.as_str();

    let subgraph_schema_name =
        get_subgraph_schema_name(&pooled_connection, &deployment_hash_string)?;

    println!("Creating entity dump directory {}", &deployment_hash_string);

    let root_path = Path::new("/tmp/graph_node/data_dump");
    let base_path = root_path.join(&deployment_id);

    let base_copy = base_path.clone();
    //Register post-run cleanup
    defer!(delete_directory(&base_copy, debug).unwrap());

    create_dump_directories(&base_path, "entities".to_string())?;
    let entity_dump_directory = base_path.join("entities/raw");
    let entity_compression_directory =
        base_path.join("entities/compressed/compressed_entities.zip");
    // Need the chain to gather the call cache
    let network_chain = get_subgraph_network_chain(&pooled_connection, &deployment_id)?;

    println!("Network chain {}", &network_chain);

    let call_cache_records =
        get_call_cache(&pooled_connection, &network_chain, first_divergent_block)?;

    dump_call_cache(&entity_dump_directory, call_cache_records)?;

    // need table names to start querying entities
    let subgraph_tables = get_subgraph_tables(&pooled_connection, &subgraph_schema_name)?;
    let mut synchronous_client = PGClient::connect(
        &format!(
            "host={} user={} password={} port={} dbname={}",
            foreign_server.host,
            foreign_server.user,
            foreign_server.password,
            foreign_server.port,
            foreign_server.dbname
        ),
        NoTls,
    )?;

    for table in &subgraph_tables {
        // Get the table records that are relevant, along with the metadata on columns.
        let (columns, records) = get_filtered_subgraph_table_rows(
            &pooled_connection,
            &mut synchronous_client,
            &table.tablename,
            &subgraph_schema_name,
            first_divergent_block,
        )?;

        // Dump to entity path
        // dump_divergent_entities(&entity_dump_directory, columns, records, &table.tablename)?;
        dump_divergent_entities_csv_writer(
            &entity_dump_directory,
            columns,
            records,
            &table.tablename,
        )?;
    }
    //Compress & Upload
    walk_and_compress(&entity_dump_directory, &entity_compression_directory)?;
    upload_file_to_endpoint(
        &entity_compression_directory,
        &host,
        indexer_id,
        dispute_id,
        "entities",
    )?;

    Ok(())
}

/// Create directories required for compression and upload
fn create_dump_directories(path: &PathBuf, kind: String) -> Result<(), anyhow::Error> {
    let dir_path = path.join(format!("{}/raw", kind));
    fs::create_dir_all(&dir_path)?;
    let compression_dir = path.join(format!("{}/compressed", kind));
    fs::create_dir_all(&compression_dir)?;

    Ok(())
}

/// Walks a directory path for all files and adds them to compressed archive.
fn walk_and_compress(src_dir: &PathBuf, dst_file: &PathBuf) -> zip::result::ZipResult<()> {
    if !Path::new(src_dir).is_dir() {
        return Err(ZipError::FileNotFound);
    }

    let path = Path::new(dst_file);
    let file = File::create(&path).unwrap();

    let walkdir = WalkDir::new(src_dir);
    let it = walkdir.into_iter();

    compress(&mut it.filter_map(|e| e.ok()), src_dir, file)?;

    Ok(())
}

/// Compress contents of a directory
fn compress<T>(
    it: &mut dyn Iterator<Item = DirEntry>,
    prefix: &PathBuf,
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

/// Remove all files in the directory after upload
fn delete_directory(directory: &PathBuf, debug: bool) -> Result<(), anyhow::Error> {
    if !debug {
        fs::remove_dir_all(directory)?;
    }
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
    name: String,
}

/// Schema is required for querying entities and POI
fn get_subgraph_schema_name(
    conn: &PooledConnection<ConnectionManager<PgConnection>>,
    deployment_id: &str,
) -> Result<String, anyhow::Error> {
    let raw_query = format!(
        "SELECT name from deployment_schemas WHERE subgraph = '{}' LIMIT 1",
        deployment_id
    );
    let name_result = sql_query(raw_query).get_result::<SubgraphNameRecord>(conn)?;

    Ok(name_result.name)
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
        "SELECT namespace from chains where name = (SELECT network from deployment_schemas WHERE subgraph = '{}' LIMIT 1)",
        deployment_id
    );
    let name_result = sql_query(raw_query).get_result::<ChainNameSpaceRecord>(conn)?;

    Ok(name_result.namespace)
}

fn dump_poi_csv_writer(
    directory: &PathBuf,
    poi_records: std::vec::Vec<PoiRecord>,
) -> Result<(), anyhow::Error> {
    println!("dumping poi");
    let filepath = directory.join("poi.tsv");

    let mut csv_writer = WriterBuilder::new().delimiter(b'\t').from_path(filepath)?;

    csv_writer.write_record(&["digest", "id", "vid", "block_range"])?;

    for poi in &poi_records {
        csv_writer.write_record(&[&poi.digest, &poi.id, &poi.vid.to_string(), &poi.block_range])?;
    }
    csv_writer.flush()?;
    return Ok(());
}
/// ## Takes a filepath and sends to the dispute service for persistence.
pub fn upload_file_to_endpoint(
    fp: &PathBuf,
    host: &str,
    indexer_id: String,
    dispute_id: String,
    kind: &str,
) -> Result<(), anyhow::Error> {
    let form = multipart::Form::new().file("file", fp)?;
    let client = Client::new();
    let res = client
        .post(format!("{}/upload-{}", host, kind))
        .header("Indexer-node", indexer_id)
        .header("Dispute-hash", dispute_id)
        .multipart(form)
        .send()?;

    if res.status().is_success() {
        println!("Success!");
    } else if res.status().is_server_error() {
        println!("server error! {:?}", res.status());
        bail!("Error `{}`", res.text()?);
    } else {
        println!("Something else happened. Status: {:?}", res.status());
        bail!("Error `{}`", res.text()?);
    }

    Ok(())
}

pub fn get_divergent_blocks(
    host: &str,
    dispute_id: &String,
    indexer_id: &String,
) -> Result<Vec<i32>, anyhow::Error> {
    #[allow(dead_code)]
    #[derive(Deserialize)]
    struct DivergentBlockResponse {
        indexer_id: String,
        divergent_blocks: Vec<i32>,
    }

    let client = Client::new();
    let res = client
        .get(format!("{}/divergent_blocks", host))
        .header("Indexer-node", indexer_id)
        .header("Dispute-hash", dispute_id)
        .send()?;

    if res.status().is_success() {
        println!("success!");
    } else if res.status().is_server_error() {
        println!("server error! {:?}", res.status());
        bail!("Error `{}`", res.text()?);
    } else {
        println!("Something else happened. Status: {:?}", res.status());
        bail!("Error `{}`", res.text()?);
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
    Ok(callcache_queryset)
}

fn dump_call_cache(
    directory: &PathBuf,
    callcache_records: std::vec::Vec<CallCacheRecord>,
) -> Result<(), anyhow::Error> {
    println!("dumping call cache");
    let filepath = directory.join("call_cache.tsv");

    let mut csv_writer_callcache = WriterBuilder::new().delimiter(b'\t').from_path(filepath)?;

    // let f = File::create(&filepath).expect("Unable to create file");
    // let mut file_buffer = BufWriter::new(f);
    csv_writer_callcache.write_record(&[
        "id",
        "return_value",
        "contract_address",
        "block_number",
    ])?;

    for cc in &callcache_records {
        csv_writer_callcache.write_record(&[
            &cc.id,
            &cc.return_value,
            &cc.contract_address,
            &cc.block_number.to_string(),
        ])?;
    }
    csv_writer_callcache.flush()?;
    return Ok(());
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
///
/// Note: there may be no return values for a table filtered on a block. In this case, eagerly return
/// Note: map null values to empty string;
fn get_filtered_subgraph_table_rows(
    conn: &PooledConnection<ConnectionManager<PgConnection>>,
    raw_client: &mut PGClient,
    table_name: &String,
    schema_name: &str,
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

    // Reserved keywords in SQL appear as subgraph tables unfortunately. I circumvent this with appending underscores
    let entity_query = format!("select * from {}.{} as {}__ WHERE ({}::int4  <@ {}__.block_range AND upper_inc({}__.block_range))\
OR ({}::int4 = lower({}__.block_range))",schema_name, table_name, table_name,divergent_block, table_name, table_name,divergent_block,table_name);

    println!("{}", entity_query);
    // simple_query returns rows as strings
    // Issue with `db error: ERROR: prepared statement "s0" already exists` when not using simple_query

    let query_vec = raw_client.simple_query(&entity_query)?;

    let mut row_entries: Vec<Vec<String>> = vec![];

    println!("{} number of rows {}", &table_name, query_vec.len());

    if query_vec.len() == 0 {
        return Ok((columns, row_entries));
    }

    for query_message in &query_vec {
        let mut single_row_vector: Vec<String> = vec![];
        match query_message {
            SimpleQueryMessage::Row(query_row) => {
                for idx in 0..(query_row.len()) {
                    let item = query_row.get(idx).unwrap_or("").to_string();
                    single_row_vector.push(item);
                }
                row_entries.push(single_row_vector);
            }
            _ => println!("Empty result"),
        }
    }

    Ok((columns, row_entries))
}

fn dump_divergent_entities_csv_writer(
    directory: &PathBuf,
    table_columns: Vec<ColumnNameAndType>,
    table_records: Vec<Vec<String>>,
    table_name: &String,
) -> Result<(), anyhow::Error> {
    println!("dumping divergent entities");

    let record_filepath = directory.join(format!("{}_entities.tsv", table_name));
    let mut csv_writer_records = WriterBuilder::new()
        .delimiter(b'\t')
        .from_path(record_filepath)?;

    let meta_filepath = directory.join(format!("{}_entities_metadata.tsv", table_name));
    let mut csv_writer_metadata = WriterBuilder::new()
        .delimiter(b'\t')
        .from_path(meta_filepath)?;

    csv_writer_metadata.write_record(&["column_name", "data_type"])?;

    let mut record_header = vec![];

    for column_data in &table_columns {
        record_header.push(column_data.column_name.clone());
        csv_writer_metadata.write_record(&[&column_data.column_name, &column_data.data_type])?;
    }
    csv_writer_metadata.flush()?;

    csv_writer_records.write_record(record_header)?;

    for record in &table_records {
        let str = record.join(",");
        println!("{}", str);

        csv_writer_records.write_record(record)?;
    }
    csv_writer_records.flush()?;

    return Ok(());
}
