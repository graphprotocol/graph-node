use anyhow::anyhow;
use async_trait::async_trait;
use diesel::sql_types::Text;
use diesel::{insert_into, update, ExpressionMethods, OptionalExtension, QueryDsl};
use diesel_async::AsyncConnection;
use diesel_async::{scoped_futures::ScopedFutureExt, RunQueryDsl};

use graph::components::store::ChainHeadStore;
use graph::data::store::ethereum::call;
use graph::env::ENV_VARS;
use graph::parking_lot::RwLock;
use graph::prelude::MetricsRegistry;
use graph::prometheus::{CounterVec, GaugeVec};
use graph::slog::Logger;
use graph::stable_hash::crypto_stable_hash;
use graph::util::herd_cache::HerdCache;

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use std::{
    collections::HashMap,
    convert::{TryFrom, TryInto},
    iter::FromIterator,
    sync::Arc,
};

use graph::blockchain::{Block, BlockHash, ChainIdentifier, ExtendedBlockPtr};
use graph::cheap_clone::CheapClone;
use graph::prelude::web3::types::{H256, U256};
use graph::prelude::{
    serde_json as json, transaction_receipt::LightTransactionReceipt, BlockNumber, BlockPtr,
    CachedEthereumCall, ChainStore as ChainStoreTrait, Error, EthereumCallCache, StoreError,
};
use graph::{ensure, internal_error};

use self::recent_blocks_cache::RecentBlocksCache;
use crate::AsyncPgConnection;
use crate::{
    block_store::ChainStatus, chain_head_listener::ChainHeadUpdateSender, pool::ConnectionPool,
};

/// Our own internal notion of a block
#[derive(Clone, Debug)]
struct JsonBlock {
    ptr: BlockPtr,
    parent_hash: BlockHash,
    data: Option<json::Value>,
}

impl JsonBlock {
    fn new(ptr: BlockPtr, parent_hash: BlockHash, data: Option<json::Value>) -> Self {
        JsonBlock {
            ptr,
            parent_hash,
            data,
        }
    }

    fn timestamp(&self) -> Option<U256> {
        self.data
            .as_ref()
            .and_then(|data| data.get("timestamp"))
            .and_then(|ts| ts.as_str())
            .and_then(|ts| U256::from_dec_str(ts).ok())
    }
}

/// Tables in the 'public' database schema that store chain-specific data
mod public {
    table! {
        ethereum_networks (name) {
            name -> Varchar,
            namespace -> Varchar,
            head_block_hash -> Nullable<Varchar>,
            head_block_number -> Nullable<BigInt>,
            net_version -> Varchar,
            genesis_block_hash -> Varchar,
            head_block_cursor -> Nullable<Varchar>,
        }
    }
}

pub use data::Storage;

/// Encapuslate access to the blocks table for a chain.
mod data {
    use crate::diesel::dsl::IntervalDsl;
    use crate::AsyncPgConnection;
    use diesel::dsl::sql;
    use diesel::insert_into;
    use diesel::sql_types::{Array, Binary, Bool, Nullable, Text};
    use diesel::{delete, sql_query, ExpressionMethods, JoinOnDsl, OptionalExtension, QueryDsl};
    use diesel::{
        deserialize::FromSql,
        pg::Pg,
        serialize::{Output, ToSql},
    };
    use diesel::{
        sql_types::{BigInt, Bytea, Integer, Jsonb},
        update,
    };
    use diesel_async::{RunQueryDsl, SimpleAsyncConnection};
    use graph::blockchain::{Block, BlockHash};
    use graph::data::store::scalar::Bytes;
    use graph::internal_error;
    use graph::prelude::ethabi::ethereum_types::H160;
    use graph::prelude::transaction_receipt::LightTransactionReceipt;
    use graph::prelude::web3::types::H256;
    use graph::prelude::{
        info, serde_json as json, BlockNumber, BlockPtr, CachedEthereumCall, Error, Logger,
        StoreError,
    };

    use std::collections::HashMap;
    use std::convert::TryFrom;
    use std::fmt;
    use std::iter::FromIterator;
    use std::str::FromStr;

    use crate::transaction_receipt::RawTransactionReceipt;

    use super::JsonBlock;

    pub(crate) const ETHEREUM_BLOCKS_TABLE_NAME: &str = "public.ethereum_blocks";

    pub(crate) const ETHEREUM_CALL_CACHE_TABLE_NAME: &str = "public.eth_call_cache";

    mod public {
        pub(super) use super::super::public::ethereum_networks;

        table! {
            ethereum_blocks (hash) {
                hash -> Varchar,
                number -> BigInt,
                parent_hash -> Nullable<Varchar>,
                network_name -> Varchar, // REFERENCES ethereum_networks (name),
                data -> Jsonb,
            }
        }

        allow_tables_to_appear_in_same_query!(ethereum_networks, ethereum_blocks);

        table! {
            /// `id` is the hash of contract address + encoded function call + block number.
            eth_call_cache (id) {
                id -> Bytea,
                return_value -> Bytea,
                contract_address -> Bytea,
                block_number -> Integer,
            }
        }

        table! {
            /// When was a cached call on a contract last used? This is useful to clean old data.
            eth_call_meta (contract_address) {
                contract_address -> Bytea,
                accessed_at -> Date,
            }
        }

        joinable!(eth_call_cache -> eth_call_meta (contract_address));
        allow_tables_to_appear_in_same_query!(eth_call_cache, eth_call_meta);
    }

    // Helper for literal SQL queries that look up a block hash
    #[derive(QueryableByName)]
    struct BlockHashText {
        #[diesel(sql_type = Text)]
        hash: String,
    }

    #[derive(QueryableByName)]
    struct BlockHashBytea {
        #[diesel(sql_type = Bytea)]
        hash: Vec<u8>,
    }

    // Like H256::from_slice, but returns an error instead of panicking
    // when `bytes` does not have the right length
    fn h256_from_bytes(bytes: &[u8]) -> Result<H256, StoreError> {
        if bytes.len() == H256::len_bytes() {
            Ok(H256::from_slice(bytes))
        } else {
            Err(internal_error!(
                "invalid H256 value `{}` has {} bytes instead of {}",
                graph::prelude::hex::encode(bytes),
                bytes.len(),
                H256::len_bytes()
            ))
        }
    }

    type DynTable = diesel_dynamic_schema::Table<String>;
    type DynColumn<ST> = diesel_dynamic_schema::Column<DynTable, &'static str, ST>;

    /// The table that holds blocks when we store a chain in its own
    /// dedicated database schema
    #[derive(Clone, Debug)]
    struct BlocksTable {
        /// The fully qualified name of the blocks table, including the
        /// schema
        qname: String,
        table: DynTable,
    }

    impl BlocksTable {
        const TABLE_NAME: &'static str = "blocks";

        fn new(namespace: &str) -> Self {
            BlocksTable {
                qname: format!("{}.{}", namespace, Self::TABLE_NAME),
                table: diesel_dynamic_schema::schema(namespace.to_string())
                    .table(Self::TABLE_NAME.to_string()),
            }
        }

        fn table(&self) -> DynTable {
            self.table.clone()
        }

        fn hash(&self) -> DynColumn<Bytea> {
            self.table.column::<Bytea, _>("hash")
        }

        fn number(&self) -> DynColumn<BigInt> {
            self.table.column::<BigInt, _>("number")
        }

        fn parent_hash(&self) -> DynColumn<Bytea> {
            self.table.column::<Bytea, _>("parent_hash")
        }

        fn data(&self) -> DynColumn<Jsonb> {
            self.table.column::<Jsonb, _>("data")
        }
    }

    #[derive(Clone, Debug)]
    struct CallMetaTable {
        qname: String,
        table: DynTable,
    }

    impl CallMetaTable {
        const TABLE_NAME: &'static str = "call_meta";
        const ACCESSED_AT: &'static str = "accessed_at";

        fn new(namespace: &str) -> Self {
            CallMetaTable {
                qname: format!("{}.{}", namespace, Self::TABLE_NAME),
                table: diesel_dynamic_schema::schema(namespace.to_string())
                    .table(Self::TABLE_NAME.to_string()),
            }
        }

        fn table(&self) -> DynTable {
            self.table.clone()
        }

        fn contract_address(&self) -> DynColumn<Bytea> {
            self.table.column::<Bytea, _>("contract_address")
        }
    }

    #[derive(Clone, Debug)]
    struct CallCacheTable {
        qname: String,
        table: DynTable,
    }

    impl CallCacheTable {
        const TABLE_NAME: &'static str = "call_cache";

        fn new(namespace: &str) -> Self {
            CallCacheTable {
                qname: format!("{}.{}", namespace, Self::TABLE_NAME),
                table: diesel_dynamic_schema::schema(namespace.to_string())
                    .table(Self::TABLE_NAME.to_string()),
            }
        }

        fn table(&self) -> DynTable {
            self.table.clone()
        }

        fn id(&self) -> DynColumn<Bytea> {
            self.table.column::<Bytea, _>("id")
        }

        fn block_number(&self) -> DynColumn<BigInt> {
            self.table.column::<BigInt, _>("block_number")
        }

        fn return_value(&self) -> DynColumn<Bytea> {
            self.table.column::<Bytea, _>("return_value")
        }

        fn contract_address(&self) -> DynColumn<Bytea> {
            self.table.column::<Bytea, _>("contract_address")
        }
    }

    #[derive(Clone, Debug)]
    pub struct Schema {
        name: String,
        blocks: BlocksTable,
        call_meta: CallMetaTable,
        call_cache: CallCacheTable,
    }

    impl Schema {
        fn new(name: String) -> Self {
            let blocks = BlocksTable::new(&name);
            let call_meta = CallMetaTable::new(&name);
            let call_cache = CallCacheTable::new(&name);
            Self {
                name,
                blocks,
                call_meta,
                call_cache,
            }
        }
    }

    #[derive(Clone, Debug, AsExpression, FromSqlRow)]
    #[diesel(sql_type = Text)]
    #[allow(clippy::large_enum_variant)]
    /// Storage for a chain. The underlying namespace (database schema) is either
    /// `public` or of the form `chain[0-9]+`.
    pub enum Storage {
        /// Chain data is stored in shared tables
        Shared,
        /// The chain has its own namespace in the database with dedicated
        /// tables
        Private(Schema),
    }

    impl fmt::Display for Storage {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            match self {
                Self::Shared => Self::PUBLIC.fmt(f),
                Self::Private(Schema { name, .. }) => name.fmt(f),
            }
        }
    }

    impl FromSql<Text, Pg> for Storage {
        fn from_sql(bytes: diesel::pg::PgValue) -> diesel::deserialize::Result<Self> {
            let s = <String as FromSql<Text, Pg>>::from_sql(bytes)?;
            Self::new(s).map_err(Into::into)
        }
    }

    impl ToSql<Text, Pg> for Storage {
        fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Pg>) -> diesel::serialize::Result {
            let s = self.to_string();
            <String as ToSql<Text, Pg>>::to_sql(&s, &mut out.reborrow())
        }
    }

    impl Storage {
        const PREFIX: &'static str = "chain";
        const PUBLIC: &'static str = "public";

        pub fn new(s: String) -> Result<Self, String> {
            if s.as_str() == Self::PUBLIC {
                return Ok(Self::Shared);
            }

            if !s.starts_with(Self::PREFIX) || s.len() <= Self::PREFIX.len() {
                return Err(s);
            }
            for c in s.chars().skip(Self::PREFIX.len()) {
                if !c.is_numeric() {
                    return Err(s);
                }
            }

            Ok(Self::Private(Schema::new(s)))
        }

        /// Create dedicated database tables for this chain if it uses
        /// `Storage::Private`. If it uses `Storage::Shared`, do nothing since
        /// a regular migration will already have created the `ethereum_blocks`
        /// table
        pub(super) async fn create(&self, conn: &mut AsyncPgConnection) -> Result<(), Error> {
            fn make_ddl(nsp: &str) -> String {
                format!(
                    "
                create schema {nsp};
                create table {nsp}.blocks (
                  hash         bytea  not null primary key,
                  number       int8  not null,
                  parent_hash  bytea  not null,
                  data         jsonb not null
                );
                create index blocks_number ON {nsp}.blocks using btree(number);

                create table {nsp}.call_cache (
                  id               bytea not null primary key,
                  return_value     bytea not null,
                  contract_address bytea not null,
                  block_number     int4 not null
                );
                create index call_cache_block_number_idx ON {nsp}.call_cache(block_number);

                create table {nsp}.call_meta (
                    contract_address bytea not null primary key,
                    accessed_at      date  not null
                );
            ",
                    nsp = nsp
                )
            }

            match self {
                Storage::Shared => Ok(()),
                Storage::Private(Schema { name, .. }) => {
                    conn.batch_execute(&make_ddl(name)).await?;
                    Ok(())
                }
            }
        }

        /// Returns a fully qualified table name to the blocks table
        #[inline]
        fn blocks_table(&self) -> &str {
            match self {
                Storage::Shared => ETHEREUM_BLOCKS_TABLE_NAME,
                Storage::Private(Schema { blocks, .. }) => &blocks.qname,
            }
        }

        pub(super) async fn drop_storage(
            &self,
            conn: &mut AsyncPgConnection,
            name: &str,
        ) -> Result<(), StoreError> {
            match &self {
                Storage::Shared => {
                    use public::ethereum_blocks as b;
                    delete(b::table.filter(b::network_name.eq(name)))
                        .execute(conn)
                        .await?;
                    Ok(())
                }
                Storage::Private(Schema { name, .. }) => {
                    conn.batch_execute(&format!("drop schema {} cascade", name))
                        .await?;
                    Ok(())
                }
            }
        }

        pub(super) async fn truncate_block_cache(
            &self,
            conn: &mut AsyncPgConnection,
        ) -> Result<(), StoreError> {
            let table_name = match &self {
                Storage::Shared => ETHEREUM_BLOCKS_TABLE_NAME,
                Storage::Private(Schema { blocks, .. }) => &blocks.qname,
            };
            conn.batch_execute(&format!("truncate table {} restart identity", table_name))
                .await?;
            Ok(())
        }

        async fn truncate_call_cache(
            &self,
            conn: &mut AsyncPgConnection,
        ) -> Result<(), StoreError> {
            let table_name = match &self {
                Storage::Shared => ETHEREUM_CALL_CACHE_TABLE_NAME,
                Storage::Private(Schema { call_cache, .. }) => &call_cache.qname,
            };
            conn.batch_execute(&format!("truncate table {} restart identity", table_name))
                .await?;
            Ok(())
        }

        pub(super) async fn cleanup_shallow_blocks(
            &self,
            conn: &mut AsyncPgConnection,
            lowest_block: i32,
        ) -> Result<(), StoreError> {
            let table_name = match &self {
                Storage::Shared => ETHEREUM_BLOCKS_TABLE_NAME,
                Storage::Private(Schema { blocks, .. }) => &blocks.qname,
            };
            conn.batch_execute(&format!(
                "delete from {} WHERE number >= {} AND data->'block'->'data' = 'null'::jsonb;",
                table_name, lowest_block,
            ))
            .await?;
            Ok(())
        }

        pub(super) async fn remove_cursor(
            &self,
            conn: &mut AsyncPgConnection,
            chain: &str,
        ) -> Result<Option<BlockNumber>, StoreError> {
            use diesel::dsl::not;
            use public::ethereum_networks as n;

            let head_block_number = update(
                n::table
                    .filter(n::name.eq(chain))
                    .filter(not(n::head_block_cursor.is_null())),
            )
            .set(n::head_block_cursor.eq(None as Option<String>))
            .returning(n::head_block_number)
            .get_result::<Option<i64>>(conn)
            .await
            .optional()?
            .flatten()
            .map(|num| num as i32);

            Ok(head_block_number)
        }

        /// Insert a block. If the table already contains a block with the
        /// same hash, then overwrite that block since it may be adding
        /// transaction receipts. If `overwrite` is `true`, overwrite a
        /// possibly existing entry. If it is `false`, keep the old entry.
        pub(super) async fn upsert_block(
            &self,
            conn: &mut AsyncPgConnection,
            chain: &str,
            block: &dyn Block,
            overwrite: bool,
        ) -> Result<(), StoreError> {
            // Hash indicating 'no parent'. It seems to be customary at
            // least on EVM-compatible chains to fill the parent hash of the
            // genesis block with this value
            const NO_PARENT: &str =
                "0000000000000000000000000000000000000000000000000000000000000000";

            let number = block.number() as i64;
            let data = block.data().expect("Failed to serialize block");
            let hash = block.hash();
            let parent_hash = block.parent_hash().unwrap_or_else(|| {
                BlockHash::try_from(NO_PARENT).expect("NO_PARENT is a valid hash")
            });

            match self {
                Storage::Shared => {
                    use public::ethereum_blocks as b;

                    let values = (
                        b::hash.eq(hash.hash_hex()),
                        b::number.eq(number),
                        b::parent_hash.eq(parent_hash.hash_hex()),
                        b::network_name.eq(chain),
                        b::data.eq(data),
                    );

                    if overwrite {
                        insert_into(b::table)
                            .values(values.clone())
                            .on_conflict(b::hash)
                            .do_update()
                            .set(values)
                            .execute(conn)
                            .await?;
                    } else {
                        insert_into(b::table)
                            .values(values.clone())
                            .on_conflict(b::hash)
                            .do_nothing()
                            .execute(conn)
                            .await?;
                    }
                }
                Storage::Private(Schema { blocks, .. }) => {
                    let query = if overwrite {
                        format!(
                            "insert into {}(hash, number, parent_hash, data) \
                             values ($1, $2, $3, $4) \
                                 on conflict(hash) \
                                 do update set number = $2, parent_hash = $3, data = $4",
                            blocks.qname,
                        )
                    } else {
                        format!(
                            "insert into {}(hash, number, parent_hash, data) \
                             values ($1, $2, $3, $4) \
                                 on conflict(hash) do nothing",
                            blocks.qname
                        )
                    };
                    sql_query(query)
                        .bind::<Bytea, _>(hash.as_slice())
                        .bind::<BigInt, _>(number)
                        .bind::<Bytea, _>(parent_hash.as_slice())
                        .bind::<Jsonb, _>(data)
                        .execute(conn)
                        .await?;
                }
            };
            Ok(())
        }

        pub(super) async fn block_ptrs_by_numbers(
            &self,
            conn: &mut AsyncPgConnection,
            chain: &str,
            numbers: &[BlockNumber],
        ) -> Result<Vec<JsonBlock>, StoreError> {
            let x = match self {
                Storage::Shared => {
                    use public::ethereum_blocks as b;

                    b::table
                        .select((
                            b::hash,
                            b::number,
                            b::parent_hash,
                            sql::<Jsonb>("coalesce(data -> 'block', data)"),
                        ))
                        .filter(b::network_name.eq(chain))
                        .filter(b::number.eq_any(Vec::from_iter(numbers.iter().map(|&n| n as i64))))
                        .load::<(BlockHash, i64, BlockHash, json::Value)>(conn)
                        .await
                }
                Storage::Private(Schema { blocks, .. }) => {
                    blocks
                        .table()
                        .select((
                            blocks.hash(),
                            blocks.number(),
                            blocks.parent_hash(),
                            sql::<Jsonb>("coalesce(data -> 'block', data)"),
                        ))
                        .filter(
                            blocks
                                .number()
                                .eq_any(Vec::from_iter(numbers.iter().map(|&n| n as i64))),
                        )
                        .load::<(BlockHash, i64, BlockHash, json::Value)>(conn)
                        .await
                }
            }?;

            Ok(x.into_iter()
                .map(|(hash, nr, parent, data)| {
                    JsonBlock::new(BlockPtr::new(hash, nr as i32), parent, Some(data))
                })
                .collect())
        }

        pub(super) async fn blocks(
            &self,
            conn: &mut AsyncPgConnection,
            chain: &str,
            hashes: &[BlockHash],
        ) -> Result<Vec<JsonBlock>, StoreError> {
            // We need to deal with chain stores where some entries have a
            // toplevel 'block' field and others directly contain what would
            // be in the 'block' field. Make sure we return the contents of
            // the 'block' field if it exists, otherwise assume the whole
            // Json object is what should be in 'block'
            //
            // see also 7736e440-4c6b-11ec-8c4d-b42e99f52061
            let x = match self {
                Storage::Shared => {
                    use public::ethereum_blocks as b;

                    b::table
                        .select((
                            b::hash,
                            b::number,
                            b::parent_hash,
                            sql::<Jsonb>("coalesce(data -> 'block', data)"),
                        ))
                        .filter(b::network_name.eq(chain))
                        .filter(
                            b::hash
                                .eq_any(Vec::from_iter(hashes.iter().map(|h| format!("{:x}", h)))),
                        )
                        .load::<(BlockHash, i64, BlockHash, json::Value)>(conn)
                        .await
                }
                Storage::Private(Schema { blocks, .. }) => {
                    blocks
                        .table()
                        .select((
                            blocks.hash(),
                            blocks.number(),
                            blocks.parent_hash(),
                            sql::<Jsonb>("coalesce(data -> 'block', data)"),
                        ))
                        .filter(
                            blocks
                                .hash()
                                .eq_any(Vec::from_iter(hashes.iter().map(|h| h.as_slice()))),
                        )
                        .load::<(BlockHash, i64, BlockHash, json::Value)>(conn)
                        .await
                }
            }?;
            Ok(x.into_iter()
                .map(|(hash, nr, parent, data)| {
                    JsonBlock::new(BlockPtr::new(hash, nr as i32), parent, Some(data))
                })
                .collect())
        }

        pub(super) async fn block_hashes_by_block_number(
            &self,
            conn: &mut AsyncPgConnection,
            chain: &str,
            number: BlockNumber,
        ) -> Result<Vec<BlockHash>, Error> {
            match self {
                Storage::Shared => {
                    use public::ethereum_blocks as b;

                    b::table
                        .select(b::hash)
                        .filter(b::network_name.eq(&chain))
                        .filter(b::number.eq(number as i64))
                        .get_results::<String>(conn)
                        .await?
                        .into_iter()
                        .map(|h| h.parse())
                        .collect::<Result<Vec<BlockHash>, _>>()
                }
                Storage::Private(Schema { blocks, .. }) => Ok(blocks
                    .table()
                    .select(blocks.hash())
                    .filter(blocks.number().eq(number as i64))
                    .get_results::<Vec<u8>>(conn)
                    .await?
                    .into_iter()
                    .map(BlockHash::from)
                    .collect::<Vec<BlockHash>>()),
            }
        }

        pub(super) async fn confirm_block_hash(
            &self,
            conn: &mut AsyncPgConnection,
            chain: &str,
            number: BlockNumber,
            hash: &BlockHash,
        ) -> Result<usize, Error> {
            let number = number as i64;

            match self {
                Storage::Shared => {
                    use public::ethereum_blocks as b;

                    let hash = format!("{:x}", hash);
                    diesel::delete(b::table)
                        .filter(b::network_name.eq(chain))
                        .filter(b::number.eq(number))
                        .filter(b::hash.ne(&hash))
                        .execute(conn)
                        .await
                        .map_err(Error::from)
                }
                Storage::Private(Schema { blocks, .. }) => {
                    let query = format!(
                        "delete from {} where number = $1 and hash != $2",
                        blocks.qname
                    );
                    sql_query(query)
                        .bind::<BigInt, _>(number)
                        .bind::<Bytea, _>(hash.as_slice())
                        .execute(conn)
                        .await
                        .map_err(Error::from)
                }
            }
        }

        /// timestamp's representation depends the blockchain::Block implementation, on
        /// ethereum this is a U256 but on different chains it will most likely be different.
        pub(super) async fn block_number(
            &self,
            conn: &mut AsyncPgConnection,
            hash: &BlockHash,
        ) -> Result<Option<(BlockNumber, Option<u64>, Option<BlockHash>)>, StoreError> {
            const TIMESTAMP_QUERY: &str =
                "coalesce(data->'block'->>'timestamp', data->>'timestamp')";

            let number = match self {
                Storage::Shared => {
                    use public::ethereum_blocks as b;

                    b::table
                        .select((
                            b::number,
                            sql::<Nullable<Text>>(TIMESTAMP_QUERY),
                            b::parent_hash,
                        ))
                        .filter(b::hash.eq(format!("{:x}", hash)))
                        .first::<(i64, Option<String>, Option<String>)>(conn)
                        .await
                        .optional()?
                        .map(|(number, ts, parent_hash)| {
                            // Convert parent_hash from Hex String to Vec<u8>
                            let parent_hash_bytes = parent_hash
                                .map(|h| hex::decode(&h).expect("Invalid hex in parent_hash"));
                            (number, ts, parent_hash_bytes)
                        })
                }
                Storage::Private(Schema { blocks, .. }) => blocks
                    .table()
                    .select((
                        blocks.number(),
                        sql::<Nullable<Text>>(TIMESTAMP_QUERY),
                        blocks.parent_hash(),
                    ))
                    .filter(blocks.hash().eq(hash.as_slice()))
                    .first::<(i64, Option<String>, Vec<u8>)>(conn)
                    .await
                    .optional()?
                    .map(|(number, ts, parent_hash)| (number, ts, Some(parent_hash))),
            };

            match number {
                None => Ok(None),
                Some((number, ts, parent_hash)) => {
                    let number = BlockNumber::try_from(number)
                        .map_err(|e| StoreError::QueryExecutionError(e.to_string()))?;
                    Ok(Some((
                        number,
                        crate::chain_store::try_parse_timestamp(ts)?,
                        parent_hash.map(BlockHash::from),
                    )))
                }
            }
        }

        pub(super) async fn block_numbers(
            &self,
            conn: &mut AsyncPgConnection,
            hashes: &[BlockHash],
        ) -> Result<HashMap<BlockHash, BlockNumber>, StoreError> {
            let pairs = match self {
                Storage::Shared => {
                    use public::ethereum_blocks as b;

                    let hashes = hashes
                        .iter()
                        .map(|h| format!("{:x}", h))
                        .collect::<Vec<String>>();

                    b::table
                        .select((b::hash, b::number))
                        .filter(b::hash.eq_any(hashes))
                        .load::<(String, i64)>(conn)
                        .await?
                        .into_iter()
                        .map(|(hash, n)| {
                            let hash = hex::decode(&hash).expect("Invalid hex in parent_hash");
                            (BlockHash::from(hash), n)
                        })
                        .collect::<Vec<_>>()
                }
                Storage::Private(Schema { blocks, .. }) => {
                    // let hashes: Vec<_> = hashes.into_iter().map(|hash| &hash.0).collect();
                    blocks
                        .table()
                        .select((blocks.hash(), blocks.number()))
                        .filter(blocks.hash().eq_any(hashes))
                        .load::<(BlockHash, i64)>(conn)
                        .await?
                }
            };

            let pairs = pairs
                .into_iter()
                .map(|(hash, number)| (hash, number as i32));
            Ok(HashMap::from_iter(pairs))
        }

        /// Find the first block that is missing from the database needed to
        /// complete the chain from block `hash` to the block with number
        /// `first_block`.
        pub(super) async fn missing_parent(
            &self,
            conn: &mut AsyncPgConnection,
            chain: &str,
            first_block: i64,
            hash: H256,
            genesis: H256,
        ) -> Result<Option<H256>, Error> {
            match self {
                Storage::Shared => {
                    // We recursively build a temp table 'chain' containing the hash and
                    // parent_hash of blocks to check. The 'last' value is used to stop
                    // the recursion and is true if one of these conditions is true:
                    //   * we are missing a parent block
                    //   * we checked the required number of blocks
                    //   * we checked the genesis block
                    const MISSING_PARENT_SQL: &str = "
            with recursive chain(hash, parent_hash, last) as (
                -- base case: look at the head candidate block
                select b.hash, b.parent_hash, false
                  from ethereum_blocks b
                 where b.network_name = $1
                   and b.hash = $2
                   and b.hash != $3
                union all
                -- recursion step: add a block whose hash is the latest parent_hash
                -- on chain
                select chain.parent_hash,
                       b.parent_hash,
                       coalesce(b.parent_hash is null
                             or b.number <= $4
                             or b.hash = $3, true)
                  from chain left outer join ethereum_blocks b
                              on chain.parent_hash = b.hash
                             and b.network_name = $1
                 where not chain.last)
             select hash
               from chain
              where chain.parent_hash is null;
            ";

                    let hash = format!("{:x}", hash);
                    let genesis = format!("{:x}", genesis);
                    let missing = sql_query(MISSING_PARENT_SQL)
                        .bind::<Text, _>(chain)
                        .bind::<Text, _>(&hash)
                        .bind::<Text, _>(&genesis)
                        .bind::<BigInt, _>(first_block)
                        .load::<BlockHashText>(conn)
                        .await?;

                    let missing = match missing.len() {
                        0 => None,
                        1 => Some(missing[0].hash.parse()?),
                        _ => {
                            unreachable!("the query can only return no or one row");
                        }
                    };
                    Ok(missing)
                }
                Storage::Private(Schema { blocks, .. }) => {
                    // This is the same as `MISSING_PARENT_SQL` above except that
                    // the blocks table has a different name and that it does
                    // not have a `network_name` column
                    let query = format!(
                        "
            with recursive chain(hash, parent_hash, last) as (
                -- base case: look at the head candidate block
                select b.hash, b.parent_hash, false
                  from {qname} b
                 where b.hash = $1
                   and b.hash != $2
                union all
                -- recursion step: add a block whose hash is the latest parent_hash
                -- on chain
                select chain.parent_hash,
                       b.parent_hash,
                       coalesce(b.parent_hash is null
                             or b.number <= $3
                             or b.hash = $2, true)
                  from chain left outer join {qname} b
                              on chain.parent_hash = b.hash
                 where not chain.last)
             select hash
               from chain
              where chain.parent_hash is null;
            ",
                        qname = blocks.qname
                    );

                    let missing = sql_query(query)
                        .bind::<Bytea, _>(hash.as_bytes())
                        .bind::<Bytea, _>(genesis.as_bytes())
                        .bind::<BigInt, _>(first_block)
                        .load::<BlockHashBytea>(conn)
                        .await?;

                    let missing = match missing.len() {
                        0 => None,
                        1 => Some(h256_from_bytes(&missing[0].hash)?),
                        _ => {
                            unreachable!("the query can only return no or one row")
                        }
                    };
                    Ok(missing)
                }
            }
        }

        /// Return the best candidate for the new chain head if there is a block
        /// with a higher block number than the current chain head. The returned
        /// value if the hash and number of the candidate and the genesis block
        /// hash for the chain
        pub(super) async fn chain_head_candidate(
            &self,
            conn: &mut AsyncPgConnection,
            chain: &str,
        ) -> Result<Option<BlockPtr>, Error> {
            use public::ethereum_networks as n;

            let head = n::table
                .filter(n::name.eq(chain))
                .select(n::head_block_number)
                .first::<Option<i64>>(conn)
                .await?
                .unwrap_or(-1);

            match self {
                Storage::Shared => {
                    use public::ethereum_blocks as b;
                    b::table
                        .filter(b::network_name.eq(chain))
                        .filter(b::number.gt(head))
                        .order_by((b::number.desc(), b::hash))
                        .select((b::hash, b::number))
                        .first::<(String, i64)>(conn)
                        .await
                        .optional()?
                        .map(|(hash, number)| BlockPtr::try_from((hash.as_str(), number)))
                        .transpose()
                }
                Storage::Private(Schema { blocks, .. }) => blocks
                    .table()
                    .filter(blocks.number().gt(head))
                    .order_by((blocks.number().desc(), blocks.hash()))
                    .select((blocks.hash(), blocks.number()))
                    .first::<(Vec<u8>, i64)>(conn)
                    .await
                    .optional()?
                    .map(|(hash, number)| BlockPtr::try_from((hash.as_slice(), number)))
                    .transpose(),
            }
        }

        fn ancestor_block_query(
            &self,
            short_circuit_predicate: &str,
            blocks_table_name: &str,
        ) -> String {
            format!(
                "
                with recursive ancestors(block_hash, block_offset) as (
                    values ($1, 0)
                    union all
                    select b.parent_hash, a.block_offset + 1
                    from ancestors a, {blocks_table_name} b
                    where a.block_hash = b.hash
                    and a.block_offset < $2
                    {short_circuit_predicate}
                )
                select a.block_hash as hash, b.number as number
                from ancestors a
                inner join {blocks_table_name} b on a.block_hash = b.hash
                order by a.block_offset desc limit 1
                ",
                blocks_table_name = blocks_table_name,
                short_circuit_predicate = short_circuit_predicate,
            )
        }

        /// Returns an ancestor of a specified block at a given offset, with an option to specify a `root` hash
        /// for a targeted search. If a `root` hash is provided, the search stops at the block whose parent hash
        /// matches the `root`.
        pub(super) async fn ancestor_block(
            &self,
            conn: &mut AsyncPgConnection,
            block_ptr: BlockPtr,
            offset: BlockNumber,
            root: Option<BlockHash>,
        ) -> Result<Option<(json::Value, BlockPtr)>, Error> {
            let short_circuit_predicate = match root {
                Some(_) => "and b.parent_hash <> $3",
                None => "",
            };

            let data_and_ptr = match self {
                Storage::Shared => {
                    let query =
                        self.ancestor_block_query(short_circuit_predicate, "ethereum_blocks");

                    // type Result = (Text, i64);
                    #[derive(QueryableByName)]
                    struct BlockHashAndNumber {
                        #[diesel(sql_type = Text)]
                        hash: String,
                        #[diesel(sql_type = BigInt)]
                        number: i64,
                    }

                    let block = match root {
                        Some(root) => sql_query(query)
                            .bind::<Text, _>(block_ptr.hash_hex())
                            .bind::<BigInt, _>(offset as i64)
                            .bind::<Text, _>(root.hash_hex())
                            .get_result::<BlockHashAndNumber>(conn),
                        None => sql_query(query)
                            .bind::<Text, _>(block_ptr.hash_hex())
                            .bind::<BigInt, _>(offset as i64)
                            .get_result::<BlockHashAndNumber>(conn),
                    }
                    .await
                    .optional()?;

                    use public::ethereum_blocks as b;

                    match block {
                        None => None,
                        Some(block) => Some((
                            b::table
                                .filter(b::hash.eq(&block.hash))
                                .select(b::data)
                                .first::<json::Value>(conn)
                                .await?,
                            BlockPtr::new(
                                BlockHash::from_str(&block.hash)?,
                                i32::try_from(block.number).unwrap(),
                            ),
                        )),
                    }
                }
                Storage::Private(Schema { blocks, .. }) => {
                    let query =
                        self.ancestor_block_query(short_circuit_predicate, blocks.qname.as_str());

                    #[derive(QueryableByName)]
                    struct BlockHashAndNumber {
                        #[diesel(sql_type = Bytea)]
                        hash: Vec<u8>,
                        #[diesel(sql_type = BigInt)]
                        number: i64,
                    }

                    let block = match &root {
                        Some(root) => sql_query(query)
                            .bind::<Bytea, _>(block_ptr.hash_slice())
                            .bind::<BigInt, _>(offset as i64)
                            .bind::<Bytea, _>(root.as_slice())
                            .get_result::<BlockHashAndNumber>(conn),
                        None => sql_query(query)
                            .bind::<Bytea, _>(block_ptr.hash_slice())
                            .bind::<BigInt, _>(offset as i64)
                            .get_result::<BlockHashAndNumber>(conn),
                    }
                    .await
                    .optional()?;

                    match block {
                        None => None,
                        Some(block) => Some((
                            blocks
                                .table()
                                .filter(blocks.hash().eq(&block.hash))
                                .select(blocks.data())
                                .first::<json::Value>(conn)
                                .await?,
                            BlockPtr::from((block.hash, block.number)),
                        )),
                    }
                }
            };

            // We need to deal with chain stores where some entries have a
            // toplevel 'blocks' field and others directly contain what
            // would be in the 'blocks' field. Make sure the value we return
            // has a 'block' entry
            //
            // see also 7736e440-4c6b-11ec-8c4d-b42e99f52061
            let data_and_ptr = {
                use graph::prelude::serde_json::json;

                data_and_ptr.map(|(data, ptr)| {
                    (
                        match data.get("block") {
                            Some(_) => data,
                            None => json!({ "block": data, "transaction_receipts": [] }),
                        },
                        ptr,
                    )
                })
            };
            Ok(data_and_ptr)
        }

        pub(super) async fn delete_blocks_before(
            &self,
            conn: &mut AsyncPgConnection,
            chain: &str,
            block: i64,
        ) -> Result<usize, Error> {
            match self {
                Storage::Shared => {
                    use public::ethereum_blocks as b;

                    diesel::delete(b::table)
                        .filter(b::network_name.eq(chain))
                        .filter(b::number.lt(block))
                        .filter(b::number.gt(0))
                        .execute(conn)
                        .await
                        .map_err(Error::from)
                }
                Storage::Private(Schema { blocks, .. }) => {
                    let query = format!(
                        "delete from {} where number < $1 and number > 0",
                        blocks.qname
                    );
                    sql_query(query)
                        .bind::<BigInt, _>(block)
                        .execute(conn)
                        .await
                        .map_err(Error::from)
                }
            }
        }

        pub(super) async fn delete_blocks_by_hash(
            &self,
            conn: &mut AsyncPgConnection,
            chain: &str,
            block_hashes: &[&H256],
        ) -> Result<usize, Error> {
            match self {
                Storage::Shared => {
                    use public::ethereum_blocks as b;

                    let hashes: Vec<String> = block_hashes
                        .iter()
                        .map(|hash| format!("{hash:x}"))
                        .collect();

                    diesel::delete(b::table)
                        .filter(b::network_name.eq(chain))
                        .filter(b::hash.eq_any(hashes))
                        .filter(b::number.gt(0)) // keep genesis
                        .execute(conn)
                        .await
                        .map_err(Error::from)
                }
                Storage::Private(Schema { blocks, .. }) => {
                    let query = format!(
                        "delete from {} where hash = any($1) and number > 0",
                        blocks.qname
                    );

                    let hashes: Vec<&[u8]> =
                        block_hashes.iter().map(|hash| hash.as_bytes()).collect();

                    sql_query(query)
                        .bind::<Array<Bytea>, _>(hashes)
                        .execute(conn)
                        .await
                        .map_err(Error::from)
                }
            }
        }

        pub(super) async fn get_call_and_access(
            &self,
            conn: &mut AsyncPgConnection,
            id: &[u8],
        ) -> Result<Option<(Bytes, bool)>, Error> {
            match self {
                Storage::Shared => {
                    use public::eth_call_cache as cache;
                    use public::eth_call_meta as meta;

                    cache::table
                        .find::<&[u8]>(id.as_ref())
                        .inner_join(meta::table)
                        .select((
                            cache::return_value,
                            sql::<Bool>("CURRENT_DATE > eth_call_meta.accessed_at"),
                        ))
                        .get_result(conn)
                        .await
                        .optional()
                        .map_err(Error::from)
                }
                Storage::Private(Schema {
                    call_cache,
                    call_meta,
                    ..
                }) => call_cache
                    .table()
                    .inner_join(
                        call_meta.table().on(call_meta
                            .contract_address()
                            .eq(call_cache.contract_address())),
                    )
                    .filter(call_cache.id().eq(id))
                    .select((
                        call_cache.return_value(),
                        sql::<Bool>(&format!(
                            "CURRENT_DATE > {}.{}",
                            CallMetaTable::TABLE_NAME,
                            CallMetaTable::ACCESSED_AT
                        )),
                    ))
                    .first::<(Vec<u8>, bool)>(conn)
                    .await
                    .optional()
                    .map_err(Error::from),
            }
            .map(|row| row.map(|(return_value, expired)| (Bytes::from(return_value), expired)))
        }

        pub(super) async fn get_calls_and_access(
            &self,
            conn: &mut AsyncPgConnection,
            ids: &[&[u8]],
        ) -> Result<Vec<(Vec<u8>, Bytes, bool)>, Error> {
            let rows = match self {
                Storage::Shared => {
                    use public::eth_call_cache as cache;
                    use public::eth_call_meta as meta;

                    cache::table
                        .inner_join(meta::table)
                        .filter(cache::id.eq_any(ids))
                        .select((
                            cache::id,
                            cache::return_value,
                            sql::<Bool>("CURRENT_DATE > eth_call_meta.accessed_at"),
                        ))
                        .load(conn)
                        .await
                        .map_err(Error::from)
                }
                Storage::Private(Schema {
                    call_cache,
                    call_meta,
                    ..
                }) => call_cache
                    .table()
                    .inner_join(
                        call_meta.table().on(call_meta
                            .contract_address()
                            .eq(call_cache.contract_address())),
                    )
                    .filter(call_cache.id().eq_any(ids))
                    .select((
                        call_cache.id(),
                        call_cache.return_value(),
                        sql::<Bool>(&format!(
                            "CURRENT_DATE > {}.{}",
                            CallMetaTable::TABLE_NAME,
                            CallMetaTable::ACCESSED_AT
                        )),
                    ))
                    .load::<(Vec<u8>, Vec<u8>, bool)>(conn)
                    .await
                    .map_err(Error::from),
            }?;
            Ok(rows
                .into_iter()
                .map(|(id, return_value, expired)| (id, Bytes::from(return_value), expired))
                .collect())
        }

        pub(super) async fn get_calls_in_block(
            &self,
            conn: &mut AsyncPgConnection,
            block_ptr: BlockPtr,
        ) -> Result<Vec<CachedEthereumCall>, Error> {
            let block_num = block_ptr.block_number();

            let rows = match self {
                Storage::Shared => {
                    use public::eth_call_cache as cache;

                    cache::table
                        .select((cache::id, cache::return_value, cache::contract_address))
                        .filter(cache::block_number.eq(block_num))
                        .order(cache::contract_address)
                        .get_results::<(Vec<u8>, Vec<u8>, Vec<u8>)>(conn)
                        .await?
                }
                Storage::Private(Schema { call_cache, .. }) => {
                    call_cache
                        .table()
                        .select((
                            call_cache.id(),
                            call_cache.return_value(),
                            call_cache.contract_address(),
                        ))
                        .filter(call_cache.block_number().eq(block_num as i64))
                        .order(call_cache.contract_address())
                        .get_results::<(Vec<u8>, Vec<u8>, Vec<u8>)>(conn)
                        .await?
                }
            };

            Ok(rows
                .into_iter()
                .map(|row| CachedEthereumCall {
                    blake3_id: row.0,
                    block_ptr: block_ptr.clone(),
                    contract_address: H160::from_slice(&row.2[..]),
                    return_value: row.1,
                })
                .collect())
        }

        pub(super) async fn clear_call_cache(
            &self,
            conn: &mut AsyncPgConnection,
            head: BlockNumber,
            from: BlockNumber,
            to: BlockNumber,
        ) -> Result<(), Error> {
            if from <= 0 && to >= head {
                // We are removing the entire cache. Truncating is much
                // faster in that case
                self.truncate_call_cache(conn).await?;
                return Ok(());
            }
            match self {
                Storage::Shared => {
                    use public::eth_call_cache as cache;
                    diesel::delete(
                        cache::table
                            .filter(cache::block_number.ge(from))
                            .filter(cache::block_number.le(to)),
                    )
                    .execute(conn)
                    .await
                    .map_err(Error::from)?;
                    Ok(())
                }
                Storage::Private(Schema { call_cache, .. }) => {
                    // Because they are dynamically defined, our private call cache tables can't
                    // implement all the required traits for deletion. This means we can't use Diesel
                    // DSL with them and must rely on the `sql_query` function instead.
                    let query = format!(
                        "delete from {} where block_number >= $1 and block_number <= $2",
                        call_cache.qname
                    );
                    sql_query(query)
                        .bind::<Integer, _>(from)
                        .bind::<Integer, _>(to)
                        .execute(conn)
                        .await
                        .map_err(Error::from)
                        .map(|_| ())
                }
            }
        }

        pub async fn clear_stale_call_cache(
            &self,
            conn: &mut AsyncPgConnection,
            logger: &Logger,
            ttl_days: i32,
            ttl_max_contracts: Option<i64>,
        ) -> Result<(), Error> {
            let mut total_calls: usize = 0;
            let mut total_contracts: i64 = 0;
            // We process contracts in batches to avoid loading too many entries into memory
            // at once. Each contract can have many calls, so we also delete calls in batches.
            // Note: The batch sizes were chosen based on experimentation. Potentially, they
            // could be made configurable via ENV vars.
            let contracts_batch_size: i64 = 2000;
            let cache_batch_size: usize = 10000;

            // Limits the number of contracts to process if ttl_max_contracts is set.
            // Used also to adjust the final batch size, so we don't process more
            // contracts than the set limit.
            let remaining_contracts = |processed: i64| -> Option<i64> {
                ttl_max_contracts.map(|limit| limit.saturating_sub(processed))
            };

            match self {
                Storage::Shared => {
                    use public::eth_call_cache as cache;
                    use public::eth_call_meta as meta;

                    loop {
                        if let Some(0) = remaining_contracts(total_contracts) {
                            info!(
                                logger,
                                "Finished cleaning call cache: deleted {} entries for {} contracts (limit reached)",
                                total_calls,
                                total_contracts
                            );
                            break;
                        }

                        let batch_limit = remaining_contracts(total_contracts)
                            .map(|left| left.min(contracts_batch_size))
                            .unwrap_or(contracts_batch_size);

                        let stale_contracts = meta::table
                            .select(meta::contract_address)
                            .filter(
                                meta::accessed_at
                                    .lt(diesel::dsl::date(diesel::dsl::now - ttl_days.days())),
                            )
                            .limit(batch_limit)
                            .get_results::<Vec<u8>>(conn)
                            .await?;

                        if stale_contracts.is_empty() {
                            info!(
                                logger,
                                "Finished cleaning call cache: deleted {} entries for {} contracts",
                                total_calls,
                                total_contracts
                            );
                            break;
                        }

                        loop {
                            let next_batch = cache::table
                                .select(cache::id)
                                .filter(cache::contract_address.eq_any(&stale_contracts))
                                .limit(cache_batch_size as i64)
                                .get_results::<Vec<u8>>(conn)
                                .await?;
                            let deleted_count =
                                diesel::delete(cache::table.filter(cache::id.eq_any(&next_batch)))
                                    .execute(conn)
                                    .await?;

                            total_calls += deleted_count;

                            if deleted_count < cache_batch_size {
                                break;
                            }
                        }

                        let deleted_contracts = diesel::delete(
                            meta::table.filter(meta::contract_address.eq_any(&stale_contracts)),
                        )
                        .execute(conn)
                        .await?;

                        total_contracts += deleted_contracts as i64;
                    }

                    Ok(())
                }
                Storage::Private(Schema {
                    call_cache,
                    call_meta,
                    ..
                }) => {
                    let select_query = format!(
                        "WITH stale_contracts AS (
                            SELECT contract_address
                            FROM {}
                            WHERE accessed_at < current_date - interval '{} days'
                            LIMIT $1
                        )
                        SELECT contract_address FROM stale_contracts",
                        call_meta.qname, ttl_days
                    );

                    let delete_cache_query = format!(
                        "WITH targets AS (
                            SELECT id
                            FROM {}
                            WHERE contract_address = ANY($1)
                            LIMIT {}
                        )
                        DELETE FROM {} USING targets
                        WHERE {}.id = targets.id",
                        call_cache.qname, cache_batch_size, call_cache.qname, call_cache.qname
                    );

                    let delete_meta_query = format!(
                        "DELETE FROM {} WHERE contract_address = ANY($1)",
                        call_meta.qname
                    );

                    #[derive(QueryableByName)]
                    struct ContractAddress {
                        #[diesel(sql_type = Bytea)]
                        contract_address: Vec<u8>,
                    }

                    loop {
                        if let Some(0) = remaining_contracts(total_contracts) {
                            info!(
                                logger,
                                "Finished cleaning call cache: deleted {} entries for {} contracts (limit reached)",
                                total_calls,
                                total_contracts
                            );
                            break;
                        }

                        let batch_limit = remaining_contracts(total_contracts)
                            .map(|left| left.min(contracts_batch_size))
                            .unwrap_or(contracts_batch_size);

                        let stale_contracts: Vec<Vec<u8>> = sql_query(&select_query)
                            .bind::<BigInt, _>(batch_limit)
                            .load::<ContractAddress>(conn)
                            .await?
                            .into_iter()
                            .map(|r| r.contract_address)
                            .collect();

                        if stale_contracts.is_empty() {
                            info!(
                                logger,
                                "Finished cleaning call cache: deleted {} entries for {} contracts",
                                total_calls,
                                total_contracts
                            );
                            break;
                        }

                        loop {
                            let deleted_count = sql_query(&delete_cache_query)
                                .bind::<Array<Bytea>, _>(&stale_contracts)
                                .execute(conn)
                                .await?;

                            total_calls += deleted_count;

                            if deleted_count < cache_batch_size {
                                break;
                            }
                        }

                        let deleted_contracts = sql_query(&delete_meta_query)
                            .bind::<Array<Bytea>, _>(&stale_contracts)
                            .execute(conn)
                            .await?;

                        total_contracts += deleted_contracts as i64;
                    }

                    Ok(())
                }
            }
        }

        pub(super) async fn update_accessed_at(
            &self,
            conn: &mut AsyncPgConnection,
            contract_address: &[u8],
        ) -> Result<(), Error> {
            let result = match self {
                Storage::Shared => {
                    use public::eth_call_meta as meta;

                    update(meta::table.find::<&[u8]>(contract_address.as_ref()))
                        .set(meta::accessed_at.eq(sql("CURRENT_DATE")))
                        .execute(conn)
                        .await
                }
                Storage::Private(Schema { call_meta, .. }) => {
                    let query = format!(
                        "update {} set accessed_at = CURRENT_DATE where contract_address = $1",
                        call_meta.qname
                    );
                    sql_query(query)
                        .bind::<Bytea, _>(contract_address)
                        .execute(conn)
                        .await
                }
            };
            result.map(|_| ()).map_err(Error::from)
        }

        pub(super) async fn set_call(
            &self,
            conn: &mut AsyncPgConnection,
            id: &[u8],
            contract_address: &[u8],
            block_number: i32,
            return_value: &[u8],
        ) -> Result<(), Error> {
            let result = match self {
                Storage::Shared => {
                    use public::eth_call_cache as cache;
                    use public::eth_call_meta as meta;

                    insert_into(cache::table)
                        .values((
                            cache::id.eq(id),
                            cache::contract_address.eq(contract_address),
                            cache::block_number.eq(block_number),
                            cache::return_value.eq(return_value),
                        ))
                        .on_conflict_do_nothing()
                        .execute(conn)
                        .await?;

                    // See comment in the Private branch for why the
                    // raciness of this check is ok
                    let update_meta = meta::table
                        .filter(meta::contract_address.eq(contract_address))
                        .select(sql::<Bool>("accessed_at < current_date"))
                        .first::<bool>(conn)
                        .await
                        .optional()?
                        .unwrap_or(true);
                    if update_meta {
                        let accessed_at = meta::accessed_at.eq(sql("CURRENT_DATE"));
                        insert_into(meta::table)
                            .values((
                                meta::contract_address.eq(contract_address),
                                accessed_at.clone(),
                            ))
                            .on_conflict(meta::contract_address)
                            .do_update()
                            .set(accessed_at)
                            // TODO: Add a where clause similar to the Private
                            // branch to avoid unnecessary updates (not entirely
                            // trivial with diesel)
                            .execute(conn)
                            .await
                    } else {
                        Ok(0)
                    }
                }
                Storage::Private(Schema {
                    call_cache,
                    call_meta,
                    ..
                }) => {
                    let query = format!(
                        "insert into {}(id, contract_address, block_number, return_value) \
                         values ($1, $2, $3, $4) on conflict do nothing",
                        call_cache.qname
                    );
                    sql_query(query)
                        .bind::<Bytea, _>(id)
                        .bind::<Bytea, _>(contract_address)
                        .bind::<Integer, _>(block_number)
                        .bind::<Bytea, _>(return_value)
                        .execute(conn)
                        .await?;

                    // Check whether we need to update `call_meta`. The
                    // check is racy, since an update can happen between the
                    // check and the insert below, but that's fine. We can
                    // tolerate a small number of redundant updates, but
                    // will still catch the majority of cases where an
                    // update is not needed
                    let update_meta = call_meta
                        .table()
                        .filter(call_meta.contract_address().eq(contract_address))
                        .select(sql::<Bool>("accessed_at < current_date"))
                        .first::<bool>(conn)
                        .await
                        .optional()?
                        .unwrap_or(true);

                    if update_meta {
                        let query = format!(
                            "insert into {}(contract_address, accessed_at) \
                         values ($1, CURRENT_DATE) \
                         on conflict(contract_address)
                         do update set accessed_at = CURRENT_DATE \
                                 where excluded.accessed_at < CURRENT_DATE",
                            call_meta.qname
                        );
                        sql_query(query)
                            .bind::<Bytea, _>(contract_address)
                            .execute(conn)
                            .await
                    } else {
                        Ok(0)
                    }
                }
            };
            result.map(|_| ()).map_err(Error::from)
        }

        #[cfg(debug_assertions)]
        // used by `super::set_chain` for test support
        pub(super) async fn remove_chain(&self, conn: &mut AsyncPgConnection, chain_name: &str) {
            match self {
                Storage::Shared => {
                    use public::eth_call_cache as c;
                    use public::eth_call_meta as m;
                    use public::ethereum_blocks as b;

                    diesel::delete(b::table.filter(b::network_name.eq(chain_name)))
                        .execute(conn)
                        .await
                        .expect("Failed to delete ethereum_blocks");
                    // We don't have a good way to clean out the call cache
                    // per chain; just nuke everything
                    diesel::delete(c::table).execute(conn).await.unwrap();
                    diesel::delete(m::table).execute(conn).await.unwrap();
                }
                Storage::Private(Schema {
                    blocks,
                    call_meta,
                    call_cache,
                    ..
                }) => {
                    for qname in &[&blocks.qname, &call_meta.qname, &call_cache.qname] {
                        let query = format!("delete from {}", qname);
                        sql_query(query)
                            .execute(conn)
                            .await
                            .unwrap_or_else(|_| panic!("Failed to delete {}", qname));
                    }
                }
            }
        }

        /// Queries the database for all the transaction receipts in a given block.
        pub(crate) async fn find_transaction_receipts_in_block(
            &self,
            conn: &mut AsyncPgConnection,
            block_hash: H256,
        ) -> anyhow::Result<Vec<LightTransactionReceipt>> {
            let query = sql_query(format!(
                "
select
    ethereum_hex_to_bytea(receipt ->> 'transactionHash') as transaction_hash,
    ethereum_hex_to_bytea(receipt ->> 'transactionIndex') as transaction_index,
    ethereum_hex_to_bytea(receipt ->> 'blockHash') as block_hash,
    ethereum_hex_to_bytea(receipt ->> 'blockNumber') as block_number,
    ethereum_hex_to_bytea(receipt ->> 'gasUsed') as gas_used,
    ethereum_hex_to_bytea(receipt ->> 'status') as status
from (
    select
        jsonb_array_elements(data -> 'transaction_receipts') as receipt
    from
        {blocks_table_name}
    where hash = $1) as temp;
",
                blocks_table_name = self.blocks_table()
            ));

            let query_results: Result<Vec<RawTransactionReceipt>, diesel::result::Error> = {
                // The `hash` column has different types between the `public.ethereum_blocks` and the
                // `chain*.blocks` tables, so we must check which one is being queried to bind the
                // `block_hash` parameter to the correct type
                match self {
                    Storage::Shared => {
                        query
                            .bind::<Text, _>(format!("{:x}", block_hash))
                            .get_results(conn)
                            .await
                    }
                    Storage::Private(_) => {
                        query
                            .bind::<Binary, _>(block_hash.as_bytes())
                            .get_results(conn)
                            .await
                    }
                }
            };
            query_results
                .map_err(|error| {
                    anyhow::anyhow!(
                        "Error fetching transaction receipt from database: {}",
                        error
                    )
                })?
                .into_iter()
                .map(LightTransactionReceipt::try_from)
                .collect()
        }
    }
}

#[derive(Debug)]
pub struct ChainStoreMetrics {
    chain_head_cache_size: Box<GaugeVec>,
    chain_head_cache_oldest_block_num: Box<GaugeVec>,
    chain_head_cache_latest_block_num: Box<GaugeVec>,
    chain_head_cache_hits: Box<CounterVec>,
    chain_head_cache_misses: Box<CounterVec>,
    // Metrics for chain_head_ptr() cache
    chain_head_ptr_cache_hits: Box<CounterVec>,
    chain_head_ptr_cache_misses: Box<CounterVec>,
    chain_head_ptr_cache_block_time_ms: Box<GaugeVec>,
}

impl ChainStoreMetrics {
    pub fn new(registry: Arc<MetricsRegistry>) -> Self {
        let chain_head_cache_size = registry
            .new_gauge_vec(
                "chain_head_cache_num_blocks",
                "Number of blocks in the chain head cache",
                vec!["network".to_string()],
            )
            .expect("Can't register the gauge");
        let chain_head_cache_oldest_block_num = registry
            .new_gauge_vec(
                "chain_head_cache_oldest_block",
                "Block number of the oldest block currently present in the chain head cache",
                vec!["network".to_string()],
            )
            .expect("Can't register the gauge");
        let chain_head_cache_latest_block_num = registry
            .new_gauge_vec(
                "chain_head_cache_latest_block",
                "Block number of the latest block currently present in the chain head cache",
                vec!["network".to_string()],
            )
            .expect("Can't register the gauge");

        let chain_head_cache_hits = registry
            .new_counter_vec(
                "chain_head_cache_hits",
                "Number of times the chain head cache was hit",
                vec!["network".to_string()],
            )
            .expect("Can't register the counter");
        let chain_head_cache_misses = registry
            .new_counter_vec(
                "chain_head_cache_misses",
                "Number of times the chain head cache was missed",
                vec!["network".to_string()],
            )
            .expect("Can't register the counter");

        let chain_head_ptr_cache_hits = registry
            .new_counter_vec(
                "chain_head_ptr_cache_hits",
                "Number of times the chain_head_ptr cache was hit",
                vec!["network".to_string()],
            )
            .expect("Can't register the counter");
        let chain_head_ptr_cache_misses = registry
            .new_counter_vec(
                "chain_head_ptr_cache_misses",
                "Number of times the chain_head_ptr cache was missed",
                vec!["network".to_string()],
            )
            .expect("Can't register the counter");
        let chain_head_ptr_cache_block_time_ms = registry
            .new_gauge_vec(
                "chain_head_ptr_cache_block_time_ms",
                "Estimated block time in milliseconds used for adaptive cache TTL",
                vec!["network".to_string()],
            )
            .expect("Can't register the gauge");

        Self {
            chain_head_cache_size,
            chain_head_cache_oldest_block_num,
            chain_head_cache_latest_block_num,
            chain_head_cache_hits,
            chain_head_cache_misses,
            chain_head_ptr_cache_hits,
            chain_head_ptr_cache_misses,
            chain_head_ptr_cache_block_time_ms,
        }
    }

    pub fn add_block(&self, network: &str) {
        self.chain_head_cache_size
            .with_label_values(&[network])
            .inc();
    }

    pub fn remove_block(&self, network: &str) {
        self.chain_head_cache_size
            .with_label_values(&[network])
            .dec();
    }

    pub fn record_cache_hit(&self, network: &str) {
        self.chain_head_cache_hits
            .get_metric_with_label_values(&[network])
            .unwrap()
            .inc();
    }

    pub fn record_cache_miss(&self, network: &str) {
        self.chain_head_cache_misses
            .get_metric_with_label_values(&[network])
            .unwrap()
            .inc();
    }

    pub fn record_hit_and_miss(&self, network: &str, hits: usize, misses: usize) {
        self.chain_head_cache_hits
            .get_metric_with_label_values(&[network])
            .unwrap()
            .inc_by(hits as f64);
        self.chain_head_cache_misses
            .get_metric_with_label_values(&[network])
            .unwrap()
            .inc_by(misses as f64);
    }

    pub fn record_chain_head_ptr_cache_hit(&self, network: &str) {
        self.chain_head_ptr_cache_hits
            .with_label_values(&[network])
            .inc();
    }

    pub fn record_chain_head_ptr_cache_miss(&self, network: &str) {
        self.chain_head_ptr_cache_misses
            .with_label_values(&[network])
            .inc();
    }

    pub fn set_chain_head_ptr_block_time(&self, network: &str, block_time_ms: u64) {
        self.chain_head_ptr_cache_block_time_ms
            .with_label_values(&[network])
            .set(block_time_ms as f64);
    }
}

const MIN_TTL_MS: u64 = 20;
const MAX_TTL_MS: u64 = 2000;
const MIN_OBSERVATIONS: u64 = 5;

/// Adaptive cache for chain_head_ptr() that learns optimal TTL from block frequency.
struct ChainHeadPtrCache {
    /// Cached value and when it expires
    entry: RwLock<Option<(BlockPtr, Instant)>>,
    /// Estimated milliseconds between blocks (EWMA)
    estimated_block_time_ms: AtomicU64,
    /// When we last observed the chain head change
    last_change: RwLock<Instant>,
    /// Number of block changes observed (for warmup)
    observations: AtomicU64,
    /// Metrics for recording cache hits/misses
    metrics: Arc<ChainStoreMetrics>,
    /// Chain name for metric labels
    chain: String,
}

impl ChainHeadPtrCache {
    fn new(metrics: Arc<ChainStoreMetrics>, chain: String) -> Self {
        Self {
            entry: RwLock::new(None),
            estimated_block_time_ms: AtomicU64::new(0),
            last_change: RwLock::new(Instant::now()),
            observations: AtomicU64::new(0),
            metrics,
            chain,
        }
    }

    /// Returns cached value if still valid, or None if cache is disabled/missed.
    /// Records hit/miss metrics automatically.
    fn get(&self) -> Option<BlockPtr> {
        if ENV_VARS.store.disable_chain_head_ptr_cache {
            return None;
        }
        let guard = self.entry.read();
        if let Some((value, expires)) = guard.as_ref() {
            if Instant::now() < *expires {
                self.metrics.record_chain_head_ptr_cache_hit(&self.chain);
                return Some(value.clone());
            }
        }
        self.metrics.record_chain_head_ptr_cache_miss(&self.chain);
        None
    }

    /// Compute current TTL - MIN_TTL during warmup, then 1/4 of estimated block time
    fn current_ttl(&self) -> Duration {
        let obs = AtomicU64::load(&self.observations, Ordering::Relaxed);
        if obs < MIN_OBSERVATIONS {
            return Duration::from_millis(MIN_TTL_MS);
        }

        let block_time = AtomicU64::load(&self.estimated_block_time_ms, Ordering::Relaxed);
        let ttl_ms = (block_time / 4).clamp(MIN_TTL_MS, MAX_TTL_MS);
        Duration::from_millis(ttl_ms)
    }

    /// Cache a new value, updating block time estimate if value changed.
    /// Does nothing if cache is disabled.
    fn set(&self, new_value: BlockPtr) {
        if ENV_VARS.store.disable_chain_head_ptr_cache {
            return;
        }
        let now = Instant::now();

        // Check if block changed
        let old_value = {
            let guard = self.entry.read();
            guard.as_ref().map(|(v, _)| v.clone())
        };

        // Only update estimate if we have a previous value and block number advanced
        // (skip reorgs where new block number <= old)
        if let Some(old_ptr) = old_value.as_ref() {
            if new_value.number > old_ptr.number {
                let mut last_change = self.last_change.write();
                let delta_ms = now.duration_since(*last_change).as_millis() as u64;
                *last_change = now;

                let blocks_advanced = (new_value.number - old_ptr.number) as u64;

                // Increment observation count
                let obs = AtomicU64::fetch_add(&self.observations, 1, Ordering::Relaxed);

                // Ignore unreasonable deltas (> 60s)
                if delta_ms > 0 && delta_ms < 60_000 {
                    let per_block_ms = delta_ms / blocks_advanced;
                    let new_estimate = if obs == 0 {
                        // First observation - use as initial estimate
                        per_block_ms
                    } else {
                        // EWMA: new = 0.8 * old + 0.2 * observed
                        let old_estimate =
                            AtomicU64::load(&self.estimated_block_time_ms, Ordering::Relaxed);
                        (old_estimate * 4 + per_block_ms) / 5
                    };
                    AtomicU64::store(
                        &self.estimated_block_time_ms,
                        new_estimate,
                        Ordering::Relaxed,
                    );

                    // Update metric gauge
                    self.metrics
                        .set_chain_head_ptr_block_time(&self.chain, new_estimate);
                }
            }
        }

        // Compute TTL and store with expiry
        let ttl = self.current_ttl();
        *self.entry.write() = Some((new_value, now + ttl));
    }
}

pub struct ChainStore {
    logger: Logger,
    pool: ConnectionPool,
    pub chain: String,
    pub(crate) storage: data::Storage,
    status: ChainStatus,
    chain_head_update_sender: ChainHeadUpdateSender,
    // TODO: We currently only use this cache for
    // [`ChainStore::ancestor_block`], but it could very well be expanded to
    // also track the network's chain head and generally improve its hit rate.
    // It is, however, quite challenging to keep the cache perfectly consistent
    // with the database and to correctly implement invalidation. So, a
    // conservative approach is acceptable.
    recent_blocks_cache: RecentBlocksCache,
    // Typed herd caches to avoid thundering herd on concurrent lookups
    blocks_by_hash_cache: HerdCache<Arc<Result<Vec<JsonBlock>, StoreError>>>,
    blocks_by_number_cache:
        HerdCache<Arc<Result<BTreeMap<BlockNumber, Vec<JsonBlock>>, StoreError>>>,
    ancestor_cache: HerdCache<Arc<Result<Option<(json::Value, BlockPtr)>, StoreError>>>,
    /// Adaptive cache for chain_head_ptr()
    chain_head_ptr_cache: ChainHeadPtrCache,
}

impl ChainStore {
    pub(crate) fn new(
        logger: Logger,
        chain: String,
        storage: data::Storage,
        status: ChainStatus,
        chain_head_update_sender: ChainHeadUpdateSender,
        pool: ConnectionPool,
        recent_blocks_cache_capacity: usize,
        metrics: Arc<ChainStoreMetrics>,
    ) -> Self {
        let recent_blocks_cache =
            RecentBlocksCache::new(recent_blocks_cache_capacity, chain.clone(), metrics.clone());
        let blocks_by_hash_cache = HerdCache::new(format!("chain_{}_blocks_by_hash", chain));
        let blocks_by_number_cache = HerdCache::new(format!("chain_{}_blocks_by_number", chain));
        let ancestor_cache = HerdCache::new(format!("chain_{}_ancestor", chain));
        let chain_head_ptr_cache = ChainHeadPtrCache::new(metrics, chain.clone());
        ChainStore {
            logger,
            pool,
            chain,
            storage,
            status,
            chain_head_update_sender,
            recent_blocks_cache,
            blocks_by_hash_cache,
            blocks_by_number_cache,
            ancestor_cache,
            chain_head_ptr_cache,
        }
    }

    pub fn is_ingestible(&self) -> bool {
        matches!(self.status, ChainStatus::Ingestible)
    }

    /// Execute a cached query, avoiding thundering herd for identical requests.
    /// Returns `(result, was_cached)`.
    async fn cached_lookup<K, T, F>(
        &self,
        cache: &HerdCache<Arc<Result<T, StoreError>>>,
        key: &K,
        lookup: F,
    ) -> (Result<T, StoreError>, bool)
    where
        K: graph::stable_hash::StableHash,
        T: Clone + Send + 'static,
        F: Future<Output = Result<T, StoreError>> + Send + 'static,
    {
        let hash = crypto_stable_hash(key);
        let lookup_fut = async move { Arc::new(lookup.await) };
        let (arc_result, cached) = cache.cached_query(hash, lookup_fut, &self.logger).await;
        let result = Arc::try_unwrap(arc_result).unwrap_or_else(|arc| (*arc).clone());
        (result, cached)
    }

    pub(crate) async fn create(&self, ident: &ChainIdentifier) -> Result<(), Error> {
        use public::ethereum_networks::dsl::*;

        let mut conn = self.pool.get_permitted().await?;
        conn.transaction(|conn| {
            async move {
                insert_into(ethereum_networks)
                    .values((
                        name.eq(&self.chain),
                        namespace.eq(&self.storage),
                        head_block_hash.eq::<Option<String>>(None),
                        head_block_number.eq::<Option<i64>>(None),
                        net_version.eq(&ident.net_version),
                        genesis_block_hash.eq(ident.genesis_block_hash.hash_hex()),
                    ))
                    .on_conflict(name)
                    .do_nothing()
                    .execute(conn)
                    .await?;
                self.storage.create(conn).await
            }
            .scope_boxed()
        })
        .await?;

        Ok(())
    }

    pub async fn update_name(&self, name: &str) -> Result<(), Error> {
        use public::ethereum_networks as n;
        let mut conn = self.pool.get_permitted().await?;
        conn.transaction(|conn| {
            async {
                update(n::table.filter(n::name.eq(&self.chain)))
                    .set(n::name.eq(name))
                    .execute(conn)
                    .await?;
                Ok(())
            }
            .scope_boxed()
        })
        .await
    }

    pub(crate) async fn drop_chain(&self) -> Result<(), Error> {
        use diesel::dsl::delete;
        use public::ethereum_networks as n;

        let mut conn = self.pool.get_permitted().await?;
        conn.transaction(|conn| {
            async {
                self.storage.drop_storage(conn, &self.chain).await?;

                delete(n::table.filter(n::name.eq(&self.chain)))
                    .execute(conn)
                    .await?;
                Ok(())
            }
            .scope_boxed()
        })
        .await
    }

    pub async fn chain_head_pointers(
        conn: &mut AsyncPgConnection,
    ) -> Result<HashMap<String, BlockPtr>, StoreError> {
        use public::ethereum_networks as n;

        let pointers: Vec<(String, BlockPtr)> = n::table
            .select((n::name, n::head_block_hash, n::head_block_number))
            .load::<(String, Option<String>, Option<i64>)>(conn)
            .await?
            .into_iter()
            .filter_map(|(name, hash, number)| match (hash, number) {
                (Some(hash), Some(number)) => Some((name, hash, number)),
                _ => None,
            })
            .map(|(name, hash, number)| {
                BlockPtr::try_from((hash.as_str(), number)).map(|ptr| (name, ptr))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(HashMap::from_iter(pointers))
    }

    pub async fn chain_head_block(&self, chain: &str) -> Result<Option<BlockNumber>, StoreError> {
        use public::ethereum_networks as n;

        let number: Option<i64> = n::table
            .filter(n::name.eq(chain))
            .select(n::head_block_number)
            .first::<Option<i64>>(&mut self.pool.get_permitted().await?)
            .await
            .optional()?
            .flatten();

        number.map(|number| number.try_into()).transpose().map_err(
            |e: std::num::TryFromIntError| {
                internal_error!(
                    "head block number for {} is {:?} which does not fit into a u32: {}",
                    chain,
                    number,
                    e.to_string()
                )
            },
        )
    }

    pub(crate) async fn set_chain_identifier(&self, ident: &ChainIdentifier) -> Result<(), Error> {
        use public::ethereum_networks as n;

        let mut conn = self.pool.get_permitted().await?;

        diesel::update(n::table.filter(n::name.eq(&self.chain)))
            .set((
                n::genesis_block_hash.eq(ident.genesis_block_hash.hash_hex()),
                n::net_version.eq(&ident.net_version),
            ))
            .execute(&mut conn)
            .await?;

        Ok(())
    }

    #[cfg(debug_assertions)]
    pub async fn set_chain_identifier_for_tests(
        &self,
        ident: &ChainIdentifier,
    ) -> Result<(), Error> {
        self.set_chain_identifier(ident).await
    }

    /// Store the given chain as the blocks for the `network` set the
    /// network's genesis block to `genesis_hash`, and head block to
    /// `null`
    #[cfg(debug_assertions)]
    pub async fn set_chain(
        &self,
        genesis_hash: &str,
        chain: Vec<Arc<dyn Block>>,
    ) -> Vec<(BlockPtr, BlockHash)> {
        let mut conn = self
            .pool
            .get()
            .await
            .expect("can get a database connection");

        self.storage.remove_chain(&mut conn, &self.chain).await;
        self.recent_blocks_cache.clear();

        for block in chain {
            self.upsert_block(block).await.expect("can upsert block");
        }

        self.set_chain_identifier(&ChainIdentifier {
            net_version: "0".to_string(),
            genesis_block_hash: BlockHash::try_from(genesis_hash).expect("valid block hash"),
        })
        .await
        .expect("unable to set chain identifier");

        use public::ethereum_networks as n;
        diesel::update(n::table.filter(n::name.eq(&self.chain)))
            .set((
                n::genesis_block_hash.eq(genesis_hash),
                n::head_block_hash.eq::<Option<&str>>(None),
                n::head_block_number.eq::<Option<i64>>(None),
            ))
            .execute(&mut conn)
            .await
            .unwrap();
        self.recent_blocks_cache.blocks()
    }

    pub async fn delete_blocks(&self, block_hashes: &[&H256]) -> Result<usize, Error> {
        let mut conn = self.pool.get_permitted().await?;
        self.storage
            .delete_blocks_by_hash(&mut conn, &self.chain, block_hashes)
            .await
    }

    pub async fn cleanup_shallow_blocks(&self, lowest_block: i32) -> Result<(), StoreError> {
        let mut conn = self.pool.get_permitted().await?;
        self.storage
            .cleanup_shallow_blocks(&mut conn, lowest_block)
            .await?;
        Ok(())
    }

    // remove_cursor delete the chain_store cursor and return true if it was present
    pub async fn remove_cursor(&self, chain: &str) -> Result<Option<BlockNumber>, StoreError> {
        let mut conn = self.pool.get_permitted().await?;
        self.storage.remove_cursor(&mut conn, chain).await
    }

    pub async fn truncate_block_cache(&self) -> Result<(), StoreError> {
        let mut conn = self.pool.get_permitted().await?;
        self.storage.truncate_block_cache(&mut conn).await?;
        Ok(())
    }

    async fn blocks_from_store(
        self: &Arc<Self>,
        hashes: Vec<BlockHash>,
    ) -> Result<Vec<JsonBlock>, StoreError> {
        let mut conn = self.pool.get_permitted().await?;
        let values = self.storage.blocks(&mut conn, &self.chain, &hashes).await?;
        Ok(values)
    }

    async fn blocks_from_store_by_numbers(
        self: &Arc<Self>,
        numbers: Vec<BlockNumber>,
    ) -> Result<BTreeMap<BlockNumber, Vec<JsonBlock>>, StoreError> {
        let mut conn = self.pool.get_permitted().await?;
        let values = self
            .storage
            .block_ptrs_by_numbers(&mut conn, &self.chain, &numbers)
            .await?;

        let mut block_map = BTreeMap::new();

        for block in values {
            let block_number = block.ptr.block_number();
            block_map
                .entry(block_number)
                .or_insert_with(Vec::new)
                .push(block);
        }

        Ok(block_map)
    }

    async fn attempt_chain_head_update_inner(
        &self,
        ancestor_count: BlockNumber,
    ) -> Result<(Option<H256>, Option<(String, i64)>), StoreError> {
        use public::ethereum_networks as n;

        let genesis_block_ptr = self.genesis_block_ptr().await?.hash_as_h256();

        let mut conn = self.pool.get_permitted().await?;
        let candidate = self
            .storage
            .chain_head_candidate(&mut conn, &self.chain)
            .await?;
        let (ptr, first_block) = match &candidate {
            None => return Ok((None, None)),
            Some(ptr) => (ptr, 0.max(ptr.number.saturating_sub(ancestor_count))),
        };

        match self
            .storage
            .missing_parent(
                &mut conn,
                &self.chain,
                first_block as i64,
                ptr.hash_as_h256(),
                genesis_block_ptr,
            )
            .await?
        {
            Some(missing) => {
                return Ok((Some(missing), None));
            }
            None => { /* we have a complete chain, no missing parents */ }
        }

        let hash = ptr.hash_hex();
        let number = ptr.number as i64;
        conn.transaction::<(Option<H256>, Option<(String, i64)>), StoreError, _>(|conn| {
            async move {
                update(n::table.filter(n::name.eq(&self.chain)))
                    .set((
                        n::head_block_hash.eq(&hash),
                        n::head_block_number.eq(number),
                    ))
                    .execute(conn)
                    .await?;
                Ok((None, Some((hash, number))))
            }
            .scope_boxed()
        })
        .await
    }

    /// Helper for tests that need to directly modify the tables for the
    /// chain store
    #[cfg(debug_assertions)]
    pub async fn get_conn_for_test(&self) -> Result<crate::pool::PermittedConnection, Error> {
        let conn = self.pool.get_permitted().await?;
        Ok(conn)
    }
}

fn json_block_to_block_ptr_ext(json_block: &JsonBlock) -> Result<ExtendedBlockPtr, Error> {
    let hash = json_block.ptr.hash.clone();
    let number = json_block.ptr.number;
    let parent_hash = json_block.parent_hash.clone();

    let timestamp = json_block
        .timestamp()
        .ok_or_else(|| anyhow!("Timestamp is missing"))?;

    let ptr =
        ExtendedBlockPtr::try_from((hash.as_h256(), number, parent_hash.as_h256(), timestamp))
            .map_err(|e| anyhow!("Failed to convert to ExtendedBlockPtr: {}", e))?;

    Ok(ptr)
}

#[async_trait]
impl ChainHeadStore for ChainStore {
    async fn chain_head_ptr(self: Arc<Self>) -> Result<Option<BlockPtr>, Error> {
        use public::ethereum_networks::dsl::*;

        // Check cache first (handles disabled check and metrics internally)
        if let Some(cached) = self.chain_head_ptr_cache.get() {
            return Ok(Some(cached));
        }

        // Query database
        let mut conn = self.pool.get_permitted().await?;
        let result = ethereum_networks
            .select((head_block_hash, head_block_number))
            .filter(name.eq(&self.chain))
            .load::<(Option<String>, Option<i64>)>(&mut conn)
            .await
            .map(|rows| {
                rows.as_slice()
                    .first()
                    .map(|(hash_opt, number_opt)| match (hash_opt, number_opt) {
                        (Some(hash), Some(number)) => Some(
                            (
                                // FIXME:
                                //
                                // workaround for arweave
                                H256::from_slice(&hex::decode(hash).unwrap()[..32]),
                                *number,
                            )
                                .into(),
                        ),
                        (None, None) => None,
                        _ => unreachable!(),
                    })
                    .and_then(|opt: Option<BlockPtr>| opt)
            })?;

        // Cache the result (set() handles disabled check internally)
        if let Some(ref ptr) = result {
            self.chain_head_ptr_cache.set(ptr.clone());
        }

        Ok(result)
    }

    async fn chain_head_cursor(&self) -> Result<Option<String>, Error> {
        use public::ethereum_networks::dsl::*;

        ethereum_networks
            .select(head_block_cursor)
            .filter(name.eq(&self.chain))
            .load::<Option<String>>(&mut self.pool.get_permitted().await?)
            .await
            .map(|rows| {
                rows.as_slice()
                    .first()
                    .map(|cursor_opt| cursor_opt.as_ref().cloned())
                    .and_then(|opt| opt)
            })
            .map_err(Error::from)
    }

    async fn set_chain_head(
        self: Arc<Self>,
        block: Arc<dyn Block>,
        cursor: String,
    ) -> Result<(), Error> {
        use public::ethereum_networks as n;

        let ptr = block.ptr();
        let hash = ptr.hash_hex();
        let number = ptr.number as i64; //block height

        //this will send an update via postgres, channel: chain_head_updates
        self.chain_head_update_sender.send(&hash, number).await?;

        let mut conn = self.pool.get_permitted().await?;
        conn.transaction(|conn| {
            async {
                self.storage
                    .upsert_block(conn, &self.chain, block.as_ref(), true)
                    .await?;

                update(n::table.filter(n::name.eq(&self.chain)))
                    .set((
                        n::head_block_hash.eq(&hash),
                        n::head_block_number.eq(number),
                        n::head_block_cursor.eq(cursor),
                    ))
                    .execute(conn)
                    .await?;

                Ok::<(), StoreError>(())
            }
            .scope_boxed()
        })
        .await?;
        Ok(())
    }
}

#[async_trait]
impl ChainStoreTrait for ChainStore {
    async fn genesis_block_ptr(&self) -> Result<BlockPtr, Error> {
        let ident = self.chain_identifier().await?;

        Ok(BlockPtr {
            hash: ident.genesis_block_hash,
            number: 0,
        })
    }

    async fn upsert_block(&self, block: Arc<dyn Block>) -> Result<(), Error> {
        // We should always have the parent block available to us at this point.
        if let Some(parent_hash) = block.parent_hash() {
            let block = JsonBlock::new(block.ptr(), parent_hash, block.data().ok());
            self.recent_blocks_cache.insert_block(block);
        }

        let mut conn = self.pool.get_permitted().await?;
        conn.transaction(|conn| {
            self.storage
                .upsert_block(conn, &self.chain, block.as_ref(), true)
                .scope_boxed()
        })
        .await
        .map_err(Error::from)
    }

    async fn upsert_light_blocks(&self, blocks: &[&dyn Block]) -> Result<(), Error> {
        let mut conn = self.pool.get_permitted().await?;
        for block in blocks {
            self.storage
                .upsert_block(&mut conn, &self.chain, *block, false)
                .await?;
        }
        Ok(())
    }

    async fn attempt_chain_head_update(
        self: Arc<Self>,
        ancestor_count: BlockNumber,
    ) -> Result<Option<H256>, Error> {
        let (missing, ptr) = self.attempt_chain_head_update_inner(ancestor_count).await?;

        if let Some((hash, number)) = ptr {
            self.chain_head_update_sender.send(&hash, number).await?;
        }

        Ok(missing)
    }

    async fn block_ptrs_by_numbers(
        self: Arc<Self>,
        numbers: Vec<BlockNumber>,
    ) -> Result<BTreeMap<BlockNumber, Vec<ExtendedBlockPtr>>, Error> {
        let result = if ENV_VARS.store.disable_block_cache_for_lookup {
            let values = self.blocks_from_store_by_numbers(numbers).await?;

            values
        } else {
            let cached = self.recent_blocks_cache.get_block_ptrs_by_numbers(&numbers);

            let stored = if cached.len() < numbers.len() {
                let missing_numbers = numbers
                    .iter()
                    .filter(|num| !cached.iter().any(|(ptr, _)| ptr.block_number() == **num))
                    .cloned()
                    .collect::<Vec<_>>();

                let this = self.clone();
                let missing_clone = missing_numbers.clone();
                let (res, _) = self
                    .cached_lookup(&self.blocks_by_number_cache, &missing_numbers, async move {
                        this.blocks_from_store_by_numbers(missing_clone).await
                    })
                    .await;

                match res {
                    Ok(blocks) => {
                        for blocks_for_num in blocks.values() {
                            if blocks.len() == 1 {
                                self.recent_blocks_cache
                                    .insert_block(blocks_for_num[0].clone());
                            }
                        }
                        blocks
                    }
                    Err(e) => {
                        return Err(e.into());
                    }
                }
            } else {
                BTreeMap::new()
            };

            let cached_map = cached
                .into_iter()
                .map(|(ptr, data)| (ptr.block_number(), vec![data]))
                .collect::<BTreeMap<_, _>>();

            let mut result = cached_map;
            for (num, blocks) in stored {
                result.entry(num).or_insert(blocks);
            }

            result
        };

        let ptrs = result
            .into_iter()
            .map(|(num, blocks)| {
                let ptrs = blocks
                    .into_iter()
                    .filter_map(|block| json_block_to_block_ptr_ext(&block).ok())
                    .collect();
                (num, ptrs)
            })
            .collect();

        Ok(ptrs)
    }

    async fn blocks(self: Arc<Self>, hashes: Vec<BlockHash>) -> Result<Vec<json::Value>, Error> {
        if ENV_VARS.store.disable_block_cache_for_lookup {
            let values = self
                .blocks_from_store(hashes)
                .await?
                .into_iter()
                .filter_map(|block| block.data)
                .collect();
            Ok(values)
        } else {
            let cached = self.recent_blocks_cache.get_blocks_by_hash(&hashes);
            let stored = if cached.len() < hashes.len() {
                let hashes = hashes
                    .iter()
                    .filter(|hash| !cached.iter().any(|(ptr, _)| &ptr.hash == *hash))
                    .cloned()
                    .collect::<Vec<_>>();
                // We key this off the entire list of hashes, which means
                // that concurrent attempts that look up `[h1, h2]` and
                // `[h1, h3]` will still run two queries and duplicate the
                // lookup of `h1`. Noticing that the two requests should be
                // serialized would require a lot more work, and going to
                // the database for one block hash, `h3`, is not much faster
                // than looking up `[h1, h3]` though it would require less
                // IO bandwidth
                let this = self.clone();
                let hashes_clone = hashes.clone();
                let (res, _) = self
                    .cached_lookup(&self.blocks_by_hash_cache, &hashes, async move {
                        this.blocks_from_store(hashes_clone).await
                    })
                    .await;

                match res {
                    Ok(blocks) => {
                        for block in &blocks {
                            self.recent_blocks_cache.insert_block(block.clone());
                        }
                        blocks
                    }
                    Err(e) => {
                        return Err(e.into());
                    }
                }
            } else {
                Vec::new()
            };

            let mut result = cached.into_iter().map(|(_, data)| data).collect::<Vec<_>>();
            let stored = stored.into_iter().filter_map(|block| block.data);
            result.extend(stored);
            Ok(result)
        }
    }

    async fn ancestor_block(
        self: Arc<Self>,
        block_ptr: BlockPtr,
        offset: BlockNumber,
        root: Option<BlockHash>,
    ) -> Result<Option<(json::Value, BlockPtr)>, Error> {
        ensure!(
            block_ptr.number >= offset,
            "block offset {} for block `{}` points to before genesis block",
            offset,
            block_ptr.hash_hex()
        );

        // Use herd cache to avoid thundering herd when multiple callers
        // request the same ancestor block simultaneously. The cache check
        // is inside the future so that only one caller checks and populates
        // the cache.
        let key = (&block_ptr, offset, &root);
        let this = self.cheap_clone();
        let block_ptr_clone = block_ptr.clone();
        let root_clone = root.clone();
        let (res, cached) = self
            .cached_lookup(&self.ancestor_cache, &key, async move {
                // Check the local cache first.
                let block_cache = this
                    .recent_blocks_cache
                    .get_ancestor(&block_ptr_clone, offset)
                    .and_then(|x| Some(x.0).zip(x.1));
                if let Some((ptr, data)) = block_cache {
                    return Ok(Some((data, ptr)));
                }

                // Cache miss, query the database
                let mut conn = this.pool.get_permitted().await?;
                let result = this
                    .storage
                    .ancestor_block(&mut conn, block_ptr_clone, offset, root_clone)
                    .await
                    .map_err(StoreError::from)?;

                // Insert into cache if we got a result
                if let Some((ref data, ref ptr)) = result {
                    // Extract parent_hash from data["block"]["parentHash"] or
                    // data["parentHash"]
                    if let Some(parent_hash) = data
                        .get("block")
                        .unwrap_or(data)
                        .get("parentHash")
                        .and_then(|h| h.as_str())
                        .and_then(|h| h.parse().ok())
                    {
                        let block = JsonBlock::new(ptr.clone(), parent_hash, Some(data.clone()));
                        this.recent_blocks_cache.insert_block(block);
                    }
                }

                Ok(result)
            })
            .await;
        let result = res?;

        if cached {
            // If we had a hit in the herd cache, we never ran the future
            // that we pass to cached_lookup but we want to pretend that we
            // actually looked the value up from the recent blocks cache
            self.recent_blocks_cache.register_hit();
        }

        Ok(result)
    }

    async fn cleanup_cached_blocks(
        &self,
        ancestor_count: BlockNumber,
    ) -> Result<Option<(BlockNumber, usize)>, Error> {
        use diesel::sql_types::Integer;

        #[derive(QueryableByName)]
        struct MinBlock {
            #[diesel(sql_type = Integer)]
            block: i32,
        }

        self.recent_blocks_cache.clear();

        // Remove all blocks from the cache that are behind the slowest
        // subgraph's head block, but retain the genesis block. We stay
        // behind the slowest subgraph so that we do not interfere with its
        // syncing activity.
        // We also stay `ancestor_count` many blocks behind the head of the
        // chain since the block ingestor consults these blocks frequently
        //
        // Only consider active subgraphs that have not failed

        // This assumes that subgraph metadata and blocks are stored in the
        // same shard. We disallow setting GRAPH_ETHEREUM_CLEANUP_BLOCKS in
        // graph_node::config so that we only run this query when we know
        // it will work. Running this with a sharded store might remove
        // blocks that are still needed by deployments in other shard
        //
        // See 8b6ad0c64e244023ac20ced7897fe666

        let mut conn = self.pool.get_permitted().await?;
        let query = "
            select coalesce(
                   least(a.block,
                        (select head_block_number::int - $1
                           from ethereum_networks
                          where name = $2)), -1)::int as block
              from (
                select min(h.block_number) as block
                  from subgraphs.deployment d,
                       subgraphs.head h,
                       subgraphs.subgraph_deployment_assignment a,
                       deployment_schemas ds
                 where ds.id = d.id
                   and h.id = d.id
                   and a.id = d.id
                   and not d.failed
                   and ds.network = $2) a;";
        let Some(block) = diesel::sql_query(query)
            .bind::<Integer, _>(ancestor_count)
            .bind::<Text, _>(&self.chain)
            .load::<MinBlock>(&mut conn)
            .await?
            .as_slice()
            .first()
            .map(|MinBlock { block }| *block)
        else {
            return Ok(None);
        };
        // If we could not determine a minimum block, the query
        // returns -1, and we should not do anything. We also guard
        // against removing the genesis block
        if block > 0 {
            self.storage
                .delete_blocks_before(&mut conn, &self.chain, block as i64)
                .await
                .map(|rows| Some((block, rows)))
        } else {
            Ok(None)
        }
    }

    async fn block_hashes_by_block_number(
        &self,
        number: BlockNumber,
    ) -> Result<Vec<BlockHash>, Error> {
        let mut conn = self.pool.get_permitted().await?;
        self.storage
            .block_hashes_by_block_number(&mut conn, &self.chain, number)
            .await
    }

    async fn confirm_block_hash(
        &self,
        number: BlockNumber,
        hash: &BlockHash,
    ) -> Result<usize, Error> {
        let mut conn = self.pool.get_permitted().await?;
        self.storage
            .confirm_block_hash(&mut conn, &self.chain, number, hash)
            .await
    }

    async fn block_number(
        &self,
        hash: &BlockHash,
    ) -> Result<Option<(String, BlockNumber, Option<u64>, Option<BlockHash>)>, StoreError> {
        let mut conn = self.pool.get_permitted().await?;
        self.storage.block_number(&mut conn, hash).await.map(|opt| {
            opt.map(|(number, timestamp, parent_hash)| {
                (self.chain.clone(), number, timestamp, parent_hash)
            })
        })
    }

    async fn block_numbers(
        &self,
        hashes: Vec<BlockHash>,
    ) -> Result<HashMap<BlockHash, BlockNumber>, StoreError> {
        if hashes.is_empty() {
            return Ok(HashMap::new());
        }

        let mut conn = self.pool.get_permitted().await?;
        self.storage
            .block_numbers(&mut conn, hashes.as_slice())
            .await
    }

    async fn clear_call_cache(&self, from: BlockNumber, to: BlockNumber) -> Result<(), Error> {
        let mut conn = self.pool.get_permitted().await?;
        if let Some(head) = self.chain_head_block(&self.chain).await? {
            self.storage
                .clear_call_cache(&mut conn, head, from, to)
                .await?;
        }
        Ok(())
    }

    async fn clear_stale_call_cache(
        &self,
        ttl_days: i32,
        ttl_max_contracts: Option<i64>,
    ) -> Result<(), Error> {
        let conn = &mut self.pool.get_permitted().await?;
        self.storage
            .clear_stale_call_cache(conn, &self.logger, ttl_days, ttl_max_contracts)
            .await
    }

    async fn transaction_receipts_in_block(
        &self,
        block_hash: &H256,
    ) -> Result<Vec<LightTransactionReceipt>, StoreError> {
        let mut conn = self.pool.get_permitted().await?;
        self.storage
            .find_transaction_receipts_in_block(&mut conn, *block_hash)
            .await
            .map_err(StoreError::from)
    }

    async fn chain_identifier(&self) -> Result<ChainIdentifier, Error> {
        let mut conn = self.pool.get_permitted().await?;
        use public::ethereum_networks as n;
        let (genesis_block_hash, net_version) = n::table
            .select((n::genesis_block_hash, n::net_version))
            .filter(n::name.eq(&self.chain))
            .get_result::<(BlockHash, String)>(&mut conn)
            .await?;

        Ok(ChainIdentifier {
            net_version,
            genesis_block_hash,
        })
    }

    fn as_head_store(self: Arc<Self>) -> Arc<dyn ChainHeadStore> {
        self.clone()
    }
}

mod recent_blocks_cache {
    use super::*;
    use std::collections::BTreeMap;

    struct Inner {
        network: String,
        metrics: Arc<ChainStoreMetrics>,
        // A list of blocks by block number. The list has at most `capacity`
        // entries. If there are multiple writes for the same block number,
        // the last one wins. Note that because of NEAR, the block numbers
        // might have gaps.
        blocks: BTreeMap<BlockNumber, JsonBlock>,
        // We only store these many blocks.
        capacity: usize,
    }

    impl Inner {
        fn get_block_by_hash(&self, hash: &BlockHash) -> Option<(&BlockPtr, &json::Value)> {
            self.blocks
                .values()
                .find(|block| &block.ptr.hash == hash)
                .and_then(|block| block.data.as_ref().map(|data| (&block.ptr, data)))
        }

        fn get_block_by_number(&self, number: BlockNumber) -> Option<&JsonBlock> {
            self.blocks.get(&number)
        }

        fn get_ancestor(
            &self,
            child_ptr: &BlockPtr,
            offset: BlockNumber,
        ) -> Option<(&BlockPtr, Option<&json::Value>)> {
            let child = self.blocks.get(&child_ptr.number)?;
            if &child.ptr != child_ptr {
                return None;
            }
            let ancestor_block_number = child.ptr.number - offset;
            let mut child = child;
            for number in (ancestor_block_number..child_ptr.number).rev() {
                let parent = self.blocks.get(&number)?;
                if child.parent_hash != parent.ptr.hash {
                    return None;
                }
                child = parent;
            }
            Some((&child.ptr, child.data.as_ref()))
        }

        fn chain_head(&self) -> Option<&BlockPtr> {
            self.blocks.last_key_value().map(|b| &b.1.ptr)
        }

        fn earliest_block(&self) -> Option<&JsonBlock> {
            self.blocks.first_key_value().map(|b| b.1)
        }

        fn evict_if_necessary(&mut self) {
            while self.blocks.len() > self.capacity {
                self.blocks.pop_first();
            }
        }

        fn update_write_metrics(&self) {
            self.metrics
                .chain_head_cache_size
                .get_metric_with_label_values(&[&self.network])
                .unwrap()
                .set(self.blocks.len() as f64);

            self.metrics
                .chain_head_cache_oldest_block_num
                .get_metric_with_label_values(&[&self.network])
                .unwrap()
                .set(self.earliest_block().map(|b| b.ptr.number).unwrap_or(0) as f64);

            self.metrics
                .chain_head_cache_latest_block_num
                .get_metric_with_label_values(&[&self.network])
                .unwrap()
                .set(self.chain_head().map(|b| b.number).unwrap_or(0) as f64);
        }

        fn insert_block(&mut self, block: JsonBlock) {
            self.blocks.insert(block.ptr.number, block);
            self.evict_if_necessary();
        }
    }

    /// We cache the most recent blocks in memory to avoid overloading the
    /// database with unnecessary queries close to the chain head. We invalidate
    /// blocks whenever the chain head advances.
    pub struct RecentBlocksCache {
        // We protect everything with a global `RwLock` to avoid data races. Ugly...
        inner: RwLock<Inner>,
    }

    impl RecentBlocksCache {
        pub fn new(capacity: usize, network: String, metrics: Arc<ChainStoreMetrics>) -> Self {
            RecentBlocksCache {
                inner: RwLock::new(Inner {
                    network,
                    metrics,
                    blocks: BTreeMap::new(),
                    capacity,
                }),
            }
        }

        pub fn register_hit(&self) {
            let inner = self.inner.read();
            inner.metrics.record_cache_hit(&inner.network);
        }

        pub fn clear(&self) {
            self.inner.write().blocks.clear();
            self.inner.read().update_write_metrics();
        }

        pub fn get_ancestor(
            &self,
            child: &BlockPtr,
            offset: BlockNumber,
        ) -> Option<(BlockPtr, Option<json::Value>)> {
            let block_opt = self
                .inner
                .read()
                .get_ancestor(child, offset)
                .map(|b| (b.0.clone(), b.1.cloned()));

            let inner = self.inner.read();
            if block_opt.is_some() {
                inner.metrics.record_cache_hit(&inner.network);
            } else {
                inner.metrics.record_cache_miss(&inner.network);
            }

            block_opt
        }

        pub fn get_blocks_by_hash(&self, hashes: &[BlockHash]) -> Vec<(BlockPtr, json::Value)> {
            let inner = self.inner.read();
            let blocks: Vec<_> = hashes
                .iter()
                .filter_map(|hash| inner.get_block_by_hash(hash))
                .map(|(ptr, value)| (ptr.clone(), value.clone()))
                .collect();
            inner.metrics.record_hit_and_miss(
                &inner.network,
                blocks.len(),
                hashes.len() - blocks.len(),
            );
            blocks
        }

        pub fn get_block_ptrs_by_numbers(
            &self,
            numbers: &[BlockNumber],
        ) -> Vec<(BlockPtr, JsonBlock)> {
            let inner = self.inner.read();
            let mut blocks: Vec<(BlockPtr, JsonBlock)> = Vec::new();

            for &number in numbers {
                if let Some(block) = inner.get_block_by_number(number) {
                    blocks.push((block.ptr.clone(), block.clone()));
                }
            }

            inner.metrics.record_hit_and_miss(
                &inner.network,
                blocks.len(),
                numbers.len() - blocks.len(),
            );

            blocks
        }

        /// Tentatively caches the `ancestor` of a [`BlockPtr`] (`child`), together with
        /// its associated `data`. Note that for this to work, `child` must be
        /// in the cache already. The first block in the cache should be
        /// inserted via [`RecentBlocksCache::set_chain_head`].
        pub(super) fn insert_block(&self, block: JsonBlock) {
            self.inner.write().insert_block(block);
            self.inner.read().update_write_metrics();
        }

        #[cfg(debug_assertions)]
        pub fn blocks(&self) -> Vec<(BlockPtr, BlockHash)> {
            self.inner
                .read()
                .blocks
                .values()
                .map(|block| (block.ptr.clone(), block.parent_hash.clone()))
                .collect()
        }
    }
}

fn try_parse_timestamp(ts: Option<String>) -> Result<Option<u64>, StoreError> {
    let ts = match ts {
        Some(str) => str,
        None => return Ok(None),
    };

    let (radix, idx) = if ts.starts_with("0x") {
        (16, 2)
    } else {
        (10, 0)
    };

    u64::from_str_radix(&ts[idx..], radix)
        .map_err(|err| {
            StoreError::QueryExecutionError(format!(
                "unexpected timestamp format {}, err: {}",
                ts, err
            ))
        })
        .map(Some)
}

#[async_trait]
impl EthereumCallCache for ChainStore {
    async fn get_call(
        &self,
        req: &call::Request,
        block: BlockPtr,
    ) -> Result<Option<call::Response>, Error> {
        if ENV_VARS.store_call_cache_disabled() {
            return Ok(None);
        }

        let id = contract_call_id(req, &block);
        let conn = &mut self.pool.get_permitted().await?;
        let return_value = conn
            .transaction::<_, Error, _>(|conn| {
                async {
                    if let Some((return_value, update_accessed_at)) =
                        self.storage.get_call_and_access(conn, id.as_ref()).await?
                    {
                        if update_accessed_at {
                            self.storage
                                .update_accessed_at(conn, req.address.as_ref())
                                .await?;
                        }
                        Ok(Some(return_value))
                    } else {
                        Ok(None)
                    }
                }
                .scope_boxed()
            })
            .await?;
        Ok(return_value.map(|return_value| {
            req.cheap_clone()
                .response(call::Retval::Value(return_value), call::Source::Store)
        }))
    }

    async fn get_calls(
        &self,
        reqs: &[call::Request],
        block: BlockPtr,
    ) -> Result<(Vec<call::Response>, Vec<call::Request>), Error> {
        if reqs.is_empty() {
            return Ok((Vec::new(), Vec::new()));
        }

        let ids: Vec<_> = reqs
            .iter()
            .map(|req| contract_call_id(req, &block))
            .collect();
        let id_refs: Vec<_> = ids.iter().map(|id| id.as_slice()).collect();

        let conn = &mut self.pool.get_permitted().await?;
        let rows = conn
            .transaction::<_, Error, _>(|conn| {
                self.storage
                    .get_calls_and_access(conn, &id_refs)
                    .scope_boxed()
            })
            .await?;

        let mut found: Vec<usize> = Vec::new();
        let mut resps = Vec::new();
        for (id, retval, _) in rows {
            let idx = ids.iter().position(|i| i.as_ref() == id).ok_or_else(|| {
                internal_error!(
                    "get_calls returned a call id that was not requested: {}",
                    hex::encode(id)
                )
            })?;
            found.push(idx);
            let resp = reqs[idx]
                .cheap_clone()
                .response(call::Retval::Value(retval), call::Source::Store);
            resps.push(resp);
        }
        let calls = reqs
            .iter()
            .enumerate()
            .filter(|(idx, _)| !found.contains(idx))
            .map(|(_, call)| call.cheap_clone())
            .collect();
        Ok((resps, calls))
    }

    async fn get_calls_in_block(&self, block: BlockPtr) -> Result<Vec<CachedEthereumCall>, Error> {
        let conn = &mut self.pool.get_permitted().await?;
        conn.transaction::<_, Error, _>(|conn| {
            self.storage.get_calls_in_block(conn, block).scope_boxed()
        })
        .await
    }

    async fn set_call(
        self: Arc<Self>,
        _: &Logger,
        call: call::Request,
        block: BlockPtr,
        return_value: call::Retval,
    ) -> Result<(), Error> {
        if ENV_VARS.store_call_cache_disabled() {
            return Ok(());
        }

        let return_value = match return_value {
            call::Retval::Value(return_value) if !return_value.is_empty() => return_value,
            _ => {
                // We do not want to cache unsuccessful calls as some RPC nodes
                // have weird behavior near the chain head. The details are lost
                // to time, but we had issues with some RPC clients in the past
                // where calls first failed and later succeeded
                // Also in some cases RPC nodes may return empty ("0x") values
                // which in the context of graph-node most likely means an issue
                // with the RPC node rather than a successful call.
                return Ok(());
            }
        };

        let id = contract_call_id(&call, &block);
        let conn = &mut self.pool.get_permitted().await?;
        conn.transaction::<_, anyhow::Error, _>(|conn| {
            self.storage
                .set_call(
                    conn,
                    id.as_ref(),
                    call.address.as_ref(),
                    block.number,
                    &return_value,
                )
                .scope_boxed()
        })
        .await?;
        Ok(())
    }
}

/// The id is the hashed encoded_call + contract_address + block hash to uniquely identify the call.
/// 256 bits of output, and therefore 128 bits of security against collisions, are needed since this
/// could be targeted by a birthday attack.
fn contract_call_id(call: &call::Request, block: &BlockPtr) -> [u8; 32] {
    let mut hash = blake3::Hasher::new();
    hash.update(&call.encoded_call);
    hash.update(call.address.as_ref());
    hash.update(block.hash_slice());
    *hash.finalize().as_bytes()
}
