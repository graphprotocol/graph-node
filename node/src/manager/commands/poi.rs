use graph::components::store::{EntityCollection, EntityFilter, EntityQuery, EntityType};
use graph::data::graphql::DocumentExt;
use graph::data::subgraph::schema::POI_OBJECT;
use graph::prelude::EntityKey;
use graph_store_postgres::BlockStore;
use graphql_parser::schema::ObjectType;
use std::io::prelude::*;
use std::io::{Seek, Write};
use std::iter::Iterator;
use std::path::{Path, PathBuf};
use walkdir::{DirEntry, WalkDir};
use zip::result::ZipError;
use zip::write::FileOptions;

use diesel::sql_types::{Integer, Text};

use graph::components::store::BlockStore as _;
use graph::data::subgraph::DeploymentHash;
use graph::prelude::anyhow::bail;
use graph::prelude::{
    anyhow,
    chrono::prelude::{DateTime, Utc},
    tokio::fs::File as TokFile,
    AttributeNames, Entity, Schema, SubgraphStore as _,
};

use graph_store_postgres::command_support::catalog::block_store;
use graph_store_postgres::{connection_pool::ConnectionPool, Store, SubgraphStore};

use serde::Deserialize;
use std::collections::HashMap;
use std::fs::{self, File};
use std::sync::Arc;

use graph::prelude::reqwest::{multipart as AsyncMultipart, Body, Client as AsyncClient};
use tokio_util::codec::{BytesCodec, FramedRead};

use csv::WriterBuilder;
/// Provide a go-like defer statement
/// https://stackoverflow.com/questions/29963449/golang-like-defer-in-rust.
/// Just a souped up closure that calls on function exit.
use std::env;

use graph_store_postgres::chain_store::CallCacheRecord;

use std::time::SystemTime;

fn iso8601(st: &std::time::SystemTime) -> String {
    let dt: DateTime<Utc> = st.clone().into();
    format!("{}", dt.format("%+"))
    // formats like "2001-07-08T00:34:60.026490+09:30"
}

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

pub async fn sync_poi(
    store: Arc<Store>,
    dispute_id: String,
    indexer_id: String,
    deployment_id: String,
    debug: bool,
    host: String,
) -> Result<(), anyhow::Error> {
    let st = SystemTime::now();
    let time_str = iso8601(&st);

    let deployment_hash = DeploymentHash::new(&deployment_id).unwrap();

    let root_path = env::temp_dir();
    fs::create_dir_all(&root_path)?;
    let base_path = root_path.join(&deployment_id).join(time_str);
    fs::create_dir_all(&base_path)?;

    let base_copy = base_path.clone();
    //Register post-run cleanup
    defer!(delete_directory(&base_copy, debug).unwrap());
    create_dump_directories(&base_path, "poi".to_string())?;

    let subgraph_store = store.subgraph_store();

    // let poi_records_db = get_poi_from_store(subgraph_store, deployment_hash)?;
    let poi_records_db = get_poi_from_store_find_query(subgraph_store, deployment_hash)?;

    let poi_records = map_db_poi(poi_records_db);

    let src_directory = base_path.join("poi/raw");
    dump_poi_csv_writer(&src_directory, poi_records)?;
    let dst_directory = base_path.join("poi/compressed/poi_compressed.zip");

    walk_and_compress(&src_directory, &dst_directory)?;
    upload_file_to_endpoint_async(&dst_directory, &host, indexer_id, dispute_id, "poi").await?;

    Ok(())
}

/// Converts Entity values pulled from the query into a struct that will serialize to csv.
fn map_db_poi(poi_db_records: Vec<Entity>) -> Vec<PoiRecord> {
    let mut poi_records: Vec<PoiRecord> = vec![];

    for poi in poi_db_records {
        let poi_record = PoiRecord {
            digest: poi
                .get("digest")
                .map(|f| f.to_string())
                .unwrap_or("".to_string()),
            id: poi
                .get("id")
                .map(|f| f.to_string())
                .unwrap_or("".to_string()),
            block_range: poi
                .get("block_range")
                .map(|f| f.to_string())
                .unwrap_or("".to_string()),
        };
        poi_records.push(poi_record);
    }

    return poi_records;
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

pub async fn sync_entities(
    store: Arc<Store>,
    block_store: Arc<BlockStore>,
    pool: ConnectionPool,
    dispute_id: String,
    indexer_id: String,
    deployment_id: String,
    debug: bool,
    host: String,
) -> Result<(), anyhow::Error> {
    use graph::prelude::QueryStoreManager;

    // Use time to create subdirectories. Keeps things idempotent.
    let st = SystemTime::now();
    let time_str = iso8601(&st);

    let pooled_connection = pool.get()?;

    let deployment_hash = DeploymentHash::new(&deployment_id.clone()).unwrap();

    // Get a connection to query store that will be passed entity queries
    let query_store = store
        .query_store(deployment_hash.clone().into(), true)
        .await?;

    //Short circuit if you error out on getting the divergent blocks
    let divergent_blocks = get_divergent_blocks(&host, &dispute_id, &indexer_id).await?;
    let first_divergent_block = match divergent_blocks.first() {
        Some(block) => *block,
        None => bail!("No divergent block"),
    };

    let root_path = env::temp_dir();
    fs::create_dir_all(&root_path)?;

    let base_path = root_path.join(&deployment_id).join(time_str);
    fs::create_dir_all(&base_path)?;

    let base_copy = base_path.clone();

    // Register post-run cleanup. Tears down directories.
    defer!(delete_directory(&base_copy, debug).unwrap());

    create_dump_directories(&base_path, "entities".to_string())?;
    let entity_dump_directory = base_path.join("entities/raw");

    let entity_compression_directory =
        base_path.join("entities/compressed/compressed_entities.zip");

    // Need the chain to gather the call cache
    let network_chain = query_store.network_name().to_owned();
    println!("Pulling from network chain {}", &network_chain);

    let chain = block_store::find_chain(&pooled_connection, &network_chain)?
        .ok_or_else(|| anyhow!("Unknown chain: {}", &network_chain))?;

    let chain_store = block_store
        .chain_store(&chain.name)
        .ok_or_else(|| anyhow!("Block store not found for chain: {}", &network_chain))?;

    // let storage = chain_store.get_storage();

    // Filter for call cache at block
    let db_call_cache_records = chain_store
        .get_calls_at_block(&pooled_connection, &first_divergent_block)
        .ok_or_else(|| {
            anyhow!(
                "Couldn't pull call cache records for block: {}",
                &first_divergent_block
            )
        })?;

    // Conversion to types amenable to csv serialization
    let mapped_call_cache = db_call_cache_records
        .iter()
        .flat_map(|cc| map_ccdb_cccsv(cc))
        .collect();

    dump_call_cache(&entity_dump_directory, mapped_call_cache)?;

    // Subgraph schema defined object types which are used to build queries.
    let subgraph_schema = _get_subgraph_schema(store.clone().subgraph_store(), &deployment_hash);

    let entity_queries =
        _get_entity_queries(subgraph_schema, deployment_hash, first_divergent_block);

    let mut object_entities: HashMap<String, Vec<Entity>> = HashMap::new();

    let mut table_typemap: HashMap<String, TableNameAndTypes> = HashMap::new();

    for (object, query) in entity_queries.clone().into_iter() {
        let object_name = &object.name.clone();
        let entities = store.subgraph_store().find_all_versions(query)?;
        object_entities.insert(object_name.clone(), entities);

        let table_types = get_subgraph_data_type(object);
        table_typemap.insert(object_name.clone(), table_types);
    }

    let its = object_entities.iter().collect::<Vec<_>>();
    println!("Serializing entities");
    for (table_name, entities) in &its {
        let table_types = table_typemap.get(*table_name).unwrap();
        write_divergent_entities(
            &entity_dump_directory,
            entities.to_vec(),
            table_name,
            &first_divergent_block,
            table_types,
        )?;
    }

    let entity_object_types: Vec<ObjectType<String>> =
        entity_queries.into_iter().map(|eq| eq.0).collect();

    let subgraph_types = get_subgraph_data_types(entity_object_types);
    for table_type in subgraph_types {
        write_column_types(&entity_dump_directory, table_type)?;
    }

    //Compress & Upload
    walk_and_compress(&entity_dump_directory, &entity_compression_directory)?;
    upload_file_to_endpoint_async(
        &entity_compression_directory,
        &host,
        indexer_id,
        dispute_id,
        "entities",
    )
    .await?;

    Ok(())
}

struct TableNameAndTypes {
    table_name: String,
    column_types: Vec<ColumnNameAndType>,
}

fn get_subgraph_data_type(object_type: ObjectType<String>) -> TableNameAndTypes {
    let data_types = get_object_data_types(&object_type);
    let table_types = TableNameAndTypes {
        table_name: object_type.name.clone(),
        column_types: data_types,
    };

    return table_types;
}

fn get_subgraph_data_types(object_types: Vec<ObjectType<String>>) -> Vec<TableNameAndTypes> {
    let mut subgraph_types: Vec<TableNameAndTypes> = vec![];

    for obj in &object_types {
        let data_types = get_object_data_types(obj);
        let table_types = TableNameAndTypes {
            table_name: obj.name.clone(),
            column_types: data_types,
        };
        subgraph_types.push(table_types);
    }
    return subgraph_types;
}

fn get_object_data_types(obj: &ObjectType<String>) -> Vec<ColumnNameAndType> {
    let mut data_types: Vec<ColumnNameAndType> = vec![];
    let fields = &obj.fields;
    for field in fields {
        let f_type = field.field_type.to_string();
        let f_name = field.name.clone();
        let column_typed = ColumnNameAndType {
            column_name: f_name,
            data_type: f_type,
        };
        data_types.push(column_typed);
    }
    return data_types;
}

fn get_poi_from_store_find_query(
    store: Arc<SubgraphStore>,
    subgraph_deployment: DeploymentHash,
) -> Result<Vec<Entity>, anyhow::Error> {
    let poi_key = EntityKey {
        subgraph_id: subgraph_deployment.clone(),
        entity_type: EntityType::from(POI_OBJECT.to_owned()),
        entity_id: "any".to_owned(),
    };

    let poi = store.select_star_entity(poi_key, subgraph_deployment)?;
    Ok(poi)
}

fn bytes_to_string(bytes: &Vec<u8>) -> Result<String, anyhow::Error> {
    use core::fmt::Write;

    let mut byte_string = String::with_capacity(2 * bytes.len());
    for byte in bytes {
        write!(byte_string, "{:02X}", byte)?;
    }
    Ok(byte_string)
}

fn map_ccdb_cccsv(input: &CallCacheRecord) -> Result<CallCacheRecordCsv, anyhow::Error> {
    let id_string = bytes_to_string(&input.id)?;
    let contract_string = bytes_to_string(&input.contract_address)?;
    let return_string = bytes_to_string(&input.return_value)?;

    let cc = CallCacheRecordCsv {
        id: id_string,
        contract_address: contract_string,
        return_value: return_string,
        block_number: input.block_number,
    };
    Ok(cc)
}

/// Create directories required for compression and upload
fn create_dump_directories(path: &PathBuf, kind: String) -> Result<(), anyhow::Error> {
    println!("Creating directory {}", path.to_str().unwrap());

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
            // println!("Compressing files {:?}", path);
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

#[derive(Queryable, QueryableByName, Deserialize)]
pub struct PoiRecord {
    #[sql_type = "Text"]
    digest: String,
    #[sql_type = "Text"]
    id: String,
    #[sql_type = "Text"]
    block_range: String,
}

/// Returns the Schema of a deployment hash.
fn _get_subgraph_schema(
    subgraph_store: Arc<SubgraphStore>,
    deployment_hash: &DeploymentHash,
) -> Arc<Schema> {
    let subgraph_schema = subgraph_store
        .input_schema(deployment_hash)
        .expect("Can't get schema for deployment_hash");
    return subgraph_schema;
}

/// Returns vector of tuples where the first item is the object in the graphql mapping (a table in Postgres)
/// and the second item is the query that can be passed to a store for gathering entities at a block.
fn _get_entity_queries(
    schema: Arc<Schema>,
    subgraph_id: DeploymentHash,
    divergent_block: i32,
) -> Vec<(ObjectType<'static, String>, EntityQuery)> {
    let types = schema.document.get_object_type_definitions();

    let mut entity_query_pair: Vec<(ObjectType<'static, String>, EntityQuery)> = vec![];

    for t in types {
        let eq = generate_query_for_type(t, &subgraph_id, divergent_block)
            .expect("Could not generate query");
        let new_tup = (t.to_owned(), eq);
        entity_query_pair.push(new_tup);
    }
    return entity_query_pair;
}

/// Returns single EntityQuery for a given object and block number.
fn generate_query_for_type(
    object_type: &ObjectType<String>,
    subgraph_id: &DeploymentHash,
    divergent_block: i32,
) -> Result<EntityQuery, anyhow::Error> {
    let obj_name = &object_type.name;
    let entity_names = vec![obj_name];
    let eq = EntityQuery::new(
        subgraph_id.clone(),
        divergent_block,
        EntityCollection::All(
            entity_names
                .into_iter()
                .map(|entity_type| {
                    (
                        EntityType::new(entity_type.to_string()),
                        AttributeNames::All,
                    )
                })
                .collect(),
        ),
    )
    .filter(EntityFilter::ChangedAtBlock(divergent_block));

    Ok(eq)
}

/// Gets the name of a chain's networnetwork_chak for a subgraph deployment.
// async fn _get_subgraph_in(
//     store: Arc<Store>,
//     deployment_id: &str,
// ) -> Result<String, anyhow::Error> {
//     use graph::prelude::QueryStoreManager;

//     let query_store = store
//         .query_store(
//             QueryTarget::Deployment(
//                 DeploymentHash::new(deployment_id).expect("valid network subgraph ID"),
//             ),
//             false,
//         )
//         .await?;

//     let name = query_store.network_name();
//     Ok(name.to_owned())
// }

fn dump_poi_csv_writer(
    directory: &PathBuf,
    poi_records: std::vec::Vec<PoiRecord>,
) -> Result<(), anyhow::Error> {
    println!("Serializing poi to file");
    let filepath = directory.join("poi.tsv");

    let mut csv_writer = WriterBuilder::new().delimiter(b'\t').from_path(filepath)?;

    csv_writer.write_record(&["digest", "id", "block_range"])?;

    for poi in &poi_records {
        csv_writer.write_record(&[&poi.digest, &poi.id, &poi.block_range])?;
    }
    csv_writer.flush()?;
    return Ok(());
}

// Wrap a file as a stream for http requests.
fn file_to_body(file: TokFile) -> Body {
    let stream = FramedRead::new(file, BytesCodec::new());
    let body = Body::wrap_stream(stream);
    body
}

// ## Takes a filepath and sends to the dispute service for persistence.
pub async fn upload_file_to_endpoint_async(
    fp: &PathBuf,
    host: &str,
    indexer_id: String,
    dispute_id: String,
    kind: &str,
) -> Result<(), anyhow::Error> {
    let file = TokFile::open(fp).await?;
    let file_name = fp
        .file_name()
        .map(|val| val.to_string_lossy().to_string())
        .unwrap_or_default();

    let client = AsyncClient::new();

    let form = AsyncMultipart::Form::new().part(
        "file",
        AsyncMultipart::Part::stream(file_to_body(file)).file_name(file_name),
    );

    let res = client
        .post(format!("{}/upload-{}", host, kind))
        .header("Indexer-node", indexer_id)
        .header("Dispute-hash", dispute_id)
        .multipart(form)
        .send()
        .await?;

    if res.status().is_success() {
        println!("Success!");
    } else if res.status().is_server_error() {
        println!("server error! {:?}", res.status());
        bail!("Error `{}`", res.text().await?);
    } else {
        println!("Something else happened. Status: {:?}", res.status());
        bail!("Error `{}`", res.text().await?);
    }

    Ok(())
}

pub async fn get_divergent_blocks(
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

    let client = AsyncClient::new();
    let res = client
        .get(format!("{}/divergent_blocks", host))
        .header("Indexer-node", indexer_id)
        .header("Dispute-hash", dispute_id)
        .send()
        .await?;

    if res.status().is_success() {
        println!("Got block!");
    } else if res.status().is_server_error() {
        println!("server error! {:?}", res.status());
        bail!("Error `{}`", res.text().await?);
    } else {
        println!("Something else happened. Status: {:?}", res.status());
        bail!("Error `{}`", res.text().await?);
    }

    let json: DivergentBlockResponse = res.json().await?;
    //get result as json and parse out divergent_blocks

    return Ok(json.divergent_blocks);
}

#[derive(Queryable, QueryableByName)]
pub struct CallCacheRecordCsv {
    #[sql_type = "Text"]
    id: String,
    #[sql_type = "Text"]
    return_value: String,
    #[sql_type = "Text"]
    contract_address: String,
    #[sql_type = "Integer"]
    block_number: i32,
}

fn dump_call_cache(
    directory: &PathBuf,
    callcache_records: std::vec::Vec<CallCacheRecordCsv>,
) -> Result<(), anyhow::Error> {
    println!("Serializing call cache");
    let filepath = directory.join("call_cache.tsv");

    let mut csv_writer_callcache = WriterBuilder::new().delimiter(b'\t').from_path(filepath)?;

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
pub struct ColumnNameAndType {
    #[sql_type = "Text"]
    column_name: String,
    #[sql_type = "Text"]
    data_type: String,
}

/// Uses fields from the entity object to index into an entity record and write to csv
fn write_divergent_entities(
    directory: &PathBuf,
    table_records: Vec<Entity>,
    table_name: &String,
    divergent_block: &i32,
    table_types: &TableNameAndTypes,
) -> Result<(), anyhow::Error> {
    // Manually save the ordering in case there's some issue with sort
    let head = table_records.first();

    match head {
        Some(_e) => {
            let record_filepath = directory.join(format!("{}_entities.tsv", table_name));
            let mut csv_writer_records = WriterBuilder::new()
                .delimiter(b'\t')
                .from_path(record_filepath)?;

            let mut header_columns: Vec<String> = table_types
                .column_types
                .iter()
                .map(|c| c.column_name.clone())
                .collect();

            header_columns.push("block_range".to_string());
            header_columns.push("filtered_block_number".to_string());

            csv_writer_records.write_record(&header_columns)?;

            for tr in table_records {
                let mut entity_as_record = vec![];
                for key in &header_columns {
                    if key == "filtered_block_number" {
                        continue;
                    }
                    let val: String = tr.get(key).map(|f| f.to_string()).unwrap_or("".to_string());
                    entity_as_record.push(val);
                }

                entity_as_record.push(divergent_block.to_string());
                csv_writer_records.write_record(entity_as_record)?;
            }
        }
        None => {}
    }

    return Ok(());
}

fn write_column_types(
    directory: &PathBuf,
    table_types: TableNameAndTypes,
) -> Result<(), anyhow::Error> {
    let meta_filepath = directory.join(format!("{}_entities_metadata.tsv", table_types.table_name));
    let mut csv_writer_metadata = WriterBuilder::new()
        .delimiter(b'\t')
        .from_path(meta_filepath)?;

    csv_writer_metadata.write_record(&["column_name", "data_type"])?;

    for column_data in &table_types.column_types {
        csv_writer_metadata.write_record(&[&column_data.column_name, &column_data.data_type])?;
    }
    csv_writer_metadata.flush()?;

    return Ok(());
}
