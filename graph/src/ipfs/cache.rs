use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::anyhow;
use async_trait::async_trait;
use bytes::Bytes;
use graph_derive::CheapClone;
use lru_time_cache::LruCache;
use object_store::{local::LocalFileSystem, path::Path, ObjectStore};
use redis::{
    aio::{ConnectionManager, ConnectionManagerConfig},
    AsyncCommands as _, RedisResult, Value,
};
use slog::{debug, info, warn, Logger};
use tokio::sync::Mutex as AsyncMutex;

use crate::{env::ENV_VARS, prelude::CheapClone};

use super::{
    ContentPath, IpfsClient, IpfsContext, IpfsError, IpfsMetrics, IpfsRequest, IpfsResponse,
    IpfsResult, RetryPolicy,
};

struct RedisClient {
    mgr: AsyncMutex<ConnectionManager>,
}

impl RedisClient {
    async fn new(logger: &Logger, path: &str) -> RedisResult<Self> {
        let env = &ENV_VARS.mappings;
        let client = redis::Client::open(path)?;
        let cfg = ConnectionManagerConfig::default()
            .set_connection_timeout(env.ipfs_timeout)
            .set_response_timeout(env.ipfs_timeout);
        info!(logger, "Connecting to Redis for IPFS caching"; "url" => path);
        // Try to connect once synchronously to check if the server is reachable.
        let _ = client.get_connection()?;
        let mgr = AsyncMutex::new(client.get_connection_manager_with_config(cfg).await?);
        info!(logger, "Connected to Redis for IPFS caching"; "url" => path);
        Ok(RedisClient { mgr })
    }

    async fn get(&self, path: &ContentPath) -> IpfsResult<Bytes> {
        let mut mgr = self.mgr.lock().await;

        let key = Self::key(path);
        let data: Vec<u8> = mgr
            .get(&key)
            .await
            .map_err(|e| IpfsError::InvalidCacheConfig {
                source: anyhow!("Failed to get IPFS object {key} from Redis cache: {e}"),
            })?;
        Ok(data.into())
    }

    async fn put(&self, path: &ContentPath, data: &Bytes) -> IpfsResult<()> {
        let mut mgr = self.mgr.lock().await;

        let key = Self::key(path);
        mgr.set(&key, data.as_ref())
            .await
            .map(|_: Value| ())
            .map_err(|e| IpfsError::InvalidCacheConfig {
                source: anyhow!("Failed to put IPFS object {key} in Redis cache: {e}"),
            })?;
        Ok(())
    }

    fn key(path: &ContentPath) -> String {
        format!("ipfs:{path}")
    }
}

#[derive(Clone, CheapClone)]
enum Cache {
    Memory {
        cache: Arc<Mutex<LruCache<ContentPath, Bytes>>>,
        max_entry_size: usize,
    },
    Disk {
        store: Arc<dyn ObjectStore>,
    },
    Redis {
        client: Arc<RedisClient>,
    },
}

fn log_object_store_err(logger: &Logger, e: &object_store::Error, log_not_found: bool) {
    if log_not_found || !matches!(e, object_store::Error::NotFound { .. }) {
        warn!(
            logger,
            "Failed to get IPFS object from disk cache; fetching from IPFS";
            "error" => e.to_string(),
        );
    }
}

fn log_redis_err(logger: &Logger, e: &IpfsError) {
    warn!(
        logger,
        "Failed to get IPFS object from Redis cache; fetching from IPFS";
        "error" => e.to_string(),
    );
}

impl Cache {
    async fn new(
        logger: &Logger,
        capacity: usize,
        max_entry_size: usize,
        path: Option<PathBuf>,
    ) -> IpfsResult<Self> {
        match path {
            Some(path) if path.starts_with("redis://") => {
                let path = path.to_string_lossy();
                let client = RedisClient::new(logger, path.as_ref())
                    .await
                    .map(Arc::new)
                    .map_err(|e| IpfsError::InvalidCacheConfig {
                        source: anyhow!("Failed to create IPFS Redis cache at {path}: {e}"),
                    })?;
                Ok(Cache::Redis { client })
            }
            Some(path) => {
                let fs = LocalFileSystem::new_with_prefix(&path).map_err(|e| {
                    IpfsError::InvalidCacheConfig {
                        source: anyhow!(
                            "Failed to create IPFS file based cache at {}: {}",
                            path.display(),
                            e
                        ),
                    }
                })?;
                debug!(logger, "Using IPFS file based cache"; "path" => path.display());
                Ok(Cache::Disk {
                    store: Arc::new(fs),
                })
            }
            None => {
                debug!(logger, "Using IPFS in-memory cache"; "capacity" => capacity, "max_entry_size" => max_entry_size);
                Ok(Self::Memory {
                    cache: Arc::new(Mutex::new(LruCache::with_capacity(capacity))),
                    max_entry_size,
                })
            }
        }
    }

    async fn find(&self, logger: &Logger, path: &ContentPath) -> Option<Bytes> {
        match self {
            Cache::Memory {
                cache,
                max_entry_size: _,
            } => cache.lock().unwrap().get(path).cloned(),
            Cache::Disk { store } => {
                let log_err = |e: &object_store::Error| log_object_store_err(logger, e, false);

                let path = Self::disk_path(path);
                let object = store.get(&path).await.inspect_err(log_err).ok()?;
                let data = object.bytes().await.inspect_err(log_err).ok()?;
                Some(data)
            }
            Cache::Redis { client } => client
                .get(path)
                .await
                .inspect_err(|e| log_redis_err(logger, e))
                .ok()
                .and_then(|data| if data.is_empty() { None } else { Some(data) }),
        }
    }

    async fn insert(&self, logger: &Logger, path: ContentPath, data: Bytes) {
        match self {
            Cache::Memory { max_entry_size, .. } if data.len() > *max_entry_size => {
                return;
            }
            Cache::Memory { cache, .. } => {
                let mut cache = cache.lock().unwrap();

                if !cache.contains_key(&path) {
                    cache.insert(path.clone(), data.clone());
                }
            }
            Cache::Disk { store } => {
                let log_err = |e: &object_store::Error| log_object_store_err(logger, e, true);
                let path = Self::disk_path(&path);
                store
                    .put(&path, data.into())
                    .await
                    .inspect_err(log_err)
                    .ok();
            }
            Cache::Redis { client } => {
                if let Err(e) = client.put(&path, &data).await {
                    log_redis_err(logger, &e);
                }
            }
        }
    }

    /// The path where we cache content on disk
    fn disk_path(path: &ContentPath) -> Path {
        Path::from(path.to_string())
    }
}

/// An IPFS client that caches the results of `cat` and `get_block` calls in
/// memory or on disk, depending on settings in the environment.
///
/// The cache is used to avoid repeated calls to the IPFS API for the same
/// content.
pub struct CachingClient {
    client: Arc<dyn IpfsClient>,
    cache: Cache,
}

impl CachingClient {
    pub async fn new(client: Arc<dyn IpfsClient>, logger: &Logger) -> IpfsResult<Self> {
        let env = &ENV_VARS.mappings;

        let cache = Cache::new(
            logger,
            env.max_ipfs_cache_size as usize,
            env.max_ipfs_cache_file_size,
            env.ipfs_cache_location.clone(),
        )
        .await?;

        Ok(CachingClient { client, cache })
    }

    async fn with_cache<F>(&self, logger: Logger, path: &ContentPath, f: F) -> IpfsResult<Bytes>
    where
        F: AsyncFnOnce() -> IpfsResult<Bytes>,
    {
        if let Some(data) = self.cache.find(&logger, path).await {
            return Ok(data);
        }

        let data = f().await?;
        self.cache.insert(&logger, path.clone(), data.clone()).await;
        Ok(data)
    }
}

#[async_trait]
impl IpfsClient for CachingClient {
    fn metrics(&self) -> &IpfsMetrics {
        self.client.metrics()
    }

    async fn call(self: Arc<Self>, req: IpfsRequest) -> IpfsResult<IpfsResponse> {
        self.client.cheap_clone().call(req).await
    }

    async fn cat(
        self: Arc<Self>,
        ctx: IpfsContext,
        path: &ContentPath,
        max_size: usize,
        timeout: Option<Duration>,
        retry_policy: RetryPolicy,
    ) -> IpfsResult<Bytes> {
        self.with_cache(ctx.logger(path), path, async || {
            {
                self.client
                    .cheap_clone()
                    .cat(ctx, path, max_size, timeout, retry_policy)
                    .await
            }
        })
        .await
    }

    async fn get_block(
        self: Arc<Self>,
        ctx: IpfsContext,
        path: &ContentPath,
        timeout: Option<Duration>,
        retry_policy: RetryPolicy,
    ) -> IpfsResult<Bytes> {
        self.with_cache(ctx.logger(path), path, async || {
            self.client
                .cheap_clone()
                .get_block(ctx, path, timeout, retry_policy)
                .await
        })
        .await
    }
}
