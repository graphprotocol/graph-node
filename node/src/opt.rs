use clap::Parser;
use git_testament::{git_testament, render_testament};
use lazy_static::lazy_static;

use crate::config;

git_testament!(TESTAMENT);
lazy_static! {
    static ref RENDERED_TESTAMENT: String = render_testament!(TESTAMENT);
}

#[derive(Clone, Debug, Parser)]
#[clap(
    name = "graph-node",
    about = "Scalable queries for a decentralized future",
    author = "Graph Protocol, Inc.",
    version = RENDERED_TESTAMENT.as_str()
)]
pub struct Opt {
    #[clap(
        long,
        env = "GRAPH_NODE_CONFIG",
        conflicts_with_all = &["postgres_url", "postgres_secondary_hosts", "postgres_host_weights"],
        required_unless_present = "postgres_url",
        help = "the name of the configuration file",
    )]
    pub config: Option<String>,
    #[clap(long, help = "validate the configuration and exit")]
    pub check_config: bool,
    #[clap(
        long,
        value_name = "[NAME:]IPFS_HASH",
        env = "SUBGRAPH",
        help = "name and IPFS hash of the subgraph manifest"
    )]
    pub subgraph: Option<String>,

    #[clap(
        long,
        env = "GRAPH_START_BLOCK",
        value_name = "BLOCK_HASH:BLOCK_NUMBER",
        help = "block hash and number that the subgraph passed will start indexing at"
    )]
    pub start_block: Option<String>,

    #[clap(
        long,
        value_name = "URL",
        env = "POSTGRES_URL",
        conflicts_with = "config",
        required_unless_present = "config",
        help = "Location of the Postgres database used for storing entities"
    )]
    pub postgres_url: Option<String>,
    #[clap(
        long,
        value_name = "URL,",
        use_value_delimiter = true,
        env = "GRAPH_POSTGRES_SECONDARY_HOSTS",
        conflicts_with = "config",
        help = "Comma-separated list of host names/IP's for read-only Postgres replicas, \
           which will share the load with the primary server"
    )]
    // FIXME: Make sure delimiter is ','
    pub postgres_secondary_hosts: Vec<String>,
    #[clap(
        long,
        value_name = "WEIGHT,",
        use_value_delimiter = true,
        env = "GRAPH_POSTGRES_HOST_WEIGHTS",
        conflicts_with = "config",
        help = "Comma-separated list of relative weights for selecting the main database \
    and secondary databases. The list is in the order MAIN,REPLICA1,REPLICA2,...\
    A host will receive approximately WEIGHT/SUM(WEIGHTS) fraction of total queries. \
    Defaults to weight 1 for each host"
    )]
    pub postgres_host_weights: Vec<usize>,
    #[clap(
        long,
        allow_negative_numbers = false,
        required_unless_present_any = &["ethereum_ws", "ethereum_ipc", "config"],
        conflicts_with_all = &["ethereum_ws", "ethereum_ipc", "config"],
        value_name="NETWORK_NAME:[CAPABILITIES]:URL",
        env="ETHEREUM_RPC",
        help= "Ethereum network name (e.g. 'mainnet'), optional comma-seperated capabilities (eg 'full,archive'), and an Ethereum RPC URL, separated by a ':'",
    )]
    pub ethereum_rpc: Vec<String>,
    #[clap(long, allow_negative_numbers = false,
        required_unless_present_any = &["ethereum_rpc", "ethereum_ipc", "config"],
        conflicts_with_all = &["ethereum_rpc", "ethereum_ipc", "config"],
        value_name="NETWORK_NAME:[CAPABILITIES]:URL",
        env="ETHEREUM_WS",
        help= "Ethereum network name (e.g. 'mainnet'), optional comma-seperated capabilities (eg 'full,archive`, and an Ethereum WebSocket URL, separated by a ':'",
    )]
    pub ethereum_ws: Vec<String>,
    #[clap(long,
        allow_negative_numbers = false,
        required_unless_present_any = &["ethereum_rpc", "ethereum_ws", "config"],
        conflicts_with_all = &["ethereum_rpc", "ethereum_ws", "config"],
        value_name="NETWORK_NAME:[CAPABILITIES]:FILE",
        env="ETHEREUM_IPC",
        help= "Ethereum network name (e.g. 'mainnet'), optional comma-seperated capabilities (eg 'full,archive'), and an Ethereum IPC pipe, separated by a ':'",
    )]
    pub ethereum_ipc: Vec<String>,
    #[clap(
        long,
        value_name = "HOST:PORT",
        env = "IPFS",
        help = "HTTP addresses of IPFS servers (RPC, Gateway)"
    )]
    pub ipfs: Vec<String>,
    #[clap(
        long,
        value_name = "{HOST:PORT|URL}",
        default_value = "https://arweave.net",
        env = "GRAPH_NODE_ARWEAVE_URL",
        help = "HTTP base URL for arweave gateway"
    )]
    pub arweave: String,
    #[clap(
        long,
        default_value = "8000",
        value_name = "PORT",
        help = "Port for the GraphQL HTTP server",
        env = "GRAPH_GRAPHQL_HTTP_PORT"
    )]
    pub http_port: u16,
    #[clap(
        long,
        default_value = "8030",
        value_name = "PORT",
        help = "Port for the index node server"
    )]
    pub index_node_port: u16,
    #[clap(
        long,
        default_value = "8020",
        value_name = "PORT",
        help = "Port for the JSON-RPC admin server"
    )]
    pub admin_port: u16,
    #[clap(
        long,
        default_value = "8040",
        value_name = "PORT",
        help = "Port for the Prometheus metrics server"
    )]
    pub metrics_port: u16,
    #[clap(
        long,
        default_value = "default",
        value_name = "NODE_ID",
        env = "GRAPH_NODE_ID",
        help = "a unique identifier for this node. Should have the same value between consecutive node restarts"
    )]
    pub node_id: String,
    #[clap(
        long,
        value_name = "FILE",
        env = "GRAPH_NODE_EXPENSIVE_QUERIES_FILE",
        default_value = "/etc/graph-node/expensive-queries.txt",
        help = "a file with a list of expensive queries, one query per line. Attempts to run these queries will return a QueryExecutionError::TooExpensive to clients"
    )]
    pub expensive_queries_filename: String,
    #[clap(long, help = "Enable debug logging")]
    pub debug: bool,

    // ============================================
    // Log Store Configuration - NEW GENERIC ARGS
    // ============================================
    #[clap(
        long = "log-store-backend",
        value_name = "BACKEND",
        help = "Log store backend to use (disabled, elasticsearch, loki, file)"
    )]
    pub log_store_backend: Option<String>,

    // --- Elasticsearch Configuration ---
    #[clap(
        long = "log-store-elasticsearch-url",
        value_name = "URL",
        help = "Elasticsearch URL for log storage"
    )]
    pub log_store_elasticsearch_url: Option<String>,
    #[clap(
        long = "log-store-elasticsearch-user",
        value_name = "USER",
        help = "Elasticsearch username for authentication"
    )]
    pub log_store_elasticsearch_user: Option<String>,
    #[clap(
        long = "log-store-elasticsearch-password",
        value_name = "PASSWORD",
        hide_env_values = true,
        help = "Elasticsearch password for authentication"
    )]
    pub log_store_elasticsearch_password: Option<String>,
    #[clap(
        long = "log-store-elasticsearch-index",
        value_name = "INDEX",
        help = "Elasticsearch index name (default: subgraph)"
    )]
    pub log_store_elasticsearch_index: Option<String>,

    // --- Loki Configuration ---
    #[clap(
        long = "log-store-loki-url",
        value_name = "URL",
        help = "Loki URL for log storage"
    )]
    pub log_store_loki_url: Option<String>,
    #[clap(
        long = "log-store-loki-tenant-id",
        value_name = "TENANT_ID",
        help = "Loki tenant ID for multi-tenancy"
    )]
    pub log_store_loki_tenant_id: Option<String>,

    // --- File Configuration ---
    #[clap(
        long = "log-store-file-dir",
        value_name = "DIR",
        help = "Directory for file-based log storage"
    )]
    pub log_store_file_dir: Option<String>,
    #[clap(
        long = "log-store-file-max-size",
        value_name = "BYTES",
        help = "Maximum log file size in bytes (default: 104857600 = 100MB)"
    )]
    pub log_store_file_max_size: Option<u64>,
    #[clap(
        long = "log-store-file-retention-days",
        value_name = "DAYS",
        help = "Number of days to retain log files (default: 30)"
    )]
    pub log_store_file_retention_days: Option<u32>,

    // ================================================
    // DEPRECATED - OLD ELASTICSEARCH-SPECIFIC ARGS
    // ================================================
    #[clap(
        long,
        value_name = "URL",
        env = "ELASTICSEARCH_URL",
        help = "DEPRECATED: Use --log-store-elasticsearch-url instead. Elasticsearch service to write subgraph logs to"
    )]
    pub elasticsearch_url: Option<String>,
    #[clap(
        long,
        value_name = "USER",
        env = "ELASTICSEARCH_USER",
        help = "DEPRECATED: Use --log-store-elasticsearch-user instead. User to use for Elasticsearch logging"
    )]
    pub elasticsearch_user: Option<String>,
    #[clap(
        long,
        value_name = "PASSWORD",
        env = "ELASTICSEARCH_PASSWORD",
        hide_env_values = true,
        help = "DEPRECATED: Use --log-store-elasticsearch-password instead. Password to use for Elasticsearch logging"
    )]
    pub elasticsearch_password: Option<String>,
    #[clap(
        long,
        value_name = "DISABLE_BLOCK_INGESTOR",
        env = "DISABLE_BLOCK_INGESTOR",
        help = "Ensures that the block ingestor component does not execute"
    )]
    pub disable_block_ingestor: bool,
    #[clap(
        long,
        value_name = "STORE_CONNECTION_POOL_SIZE",
        default_value = "10",
        env = "STORE_CONNECTION_POOL_SIZE",
        help = "Limits the number of connections in the store's connection pool"
    )]
    pub store_connection_pool_size: u32,
    #[clap(
        long,
        help = "Allows setting configurations that may result in incorrect Proofs of Indexing."
    )]
    pub unsafe_config: bool,

    #[clap(
        long,
        value_name = "IPFS_HASH",
        env = "GRAPH_DEBUG_FORK",
        help = "IPFS hash of the subgraph manifest that you want to fork"
    )]
    pub debug_fork: Option<String>,

    #[clap(
        long,
        value_name = "URL",
        env = "GRAPH_FORK_BASE",
        help = "Base URL for forking subgraphs"
    )]
    pub fork_base: Option<String>,
    #[clap(
        long,
        default_value = "8050",
        value_name = "GRAPHMAN_PORT",
        help = "Port for the graphman GraphQL server"
    )]
    pub graphman_port: u16,
}

impl From<Opt> for config::Opt {
    fn from(opt: Opt) -> Self {
        let Opt {
            postgres_url,
            config,
            store_connection_pool_size,
            postgres_host_weights,
            postgres_secondary_hosts,
            disable_block_ingestor,
            node_id,
            ethereum_rpc,
            ethereum_ws,
            ethereum_ipc,
            unsafe_config,
            ..
        } = opt;

        config::Opt {
            postgres_url,
            config,
            store_connection_pool_size,
            postgres_host_weights,
            postgres_secondary_hosts,
            disable_block_ingestor,
            node_id,
            ethereum_rpc,
            ethereum_ws,
            ethereum_ipc,
            unsafe_config,
        }
    }
}
