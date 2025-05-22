use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use bytes::Bytes;
use graph_derive::CheapClone;
use lru_time_cache::LruCache;
use object_store::{local::LocalFileSystem, path::Path, ObjectStore};
use slog::{warn, Logger};

use crate::{env::ENV_VARS, prelude::CheapClone};

use super::{ContentPath, IpfsClient, IpfsRequest, IpfsResponse, IpfsResult, RetryPolicy};

#[derive(Clone, CheapClone)]
enum Cache {
    Memory {
        cache: Arc<Mutex<LruCache<ContentPath, Bytes>>>,
        max_entry_size: usize,
    },
    Disk {
        store: Arc<dyn ObjectStore>,
    },
}

fn log_err(logger: &Logger, e: &object_store::Error, log_not_found: bool) {
    if log_not_found || !matches!(e, object_store::Error::NotFound { .. }) {
        warn!(
            logger,
            "Failed to get IPFS object from disk cache; fetching from IPFS";
            "error" => e.to_string(),
        );
    }
}

impl Cache {
    fn new(capacity: usize, max_entry_size: usize, path: Option<PathBuf>) -> Self {
        match path {
            Some(path) => {
                let fs = match LocalFileSystem::new_with_prefix(&path) {
                    Err(e) => {
                        panic!(
                            "Failed to create IPFS file based cache at {}: {}",
                            path.display(),
                            e
                        );
                    }
                    Ok(fs) => fs,
                };
                Cache::Disk {
                    store: Arc::new(fs),
                }
            }
            None => Self::Memory {
                cache: Arc::new(Mutex::new(LruCache::with_capacity(capacity))),
                max_entry_size,
            },
        }
    }

    async fn find(&self, logger: &Logger, path: &ContentPath) -> Option<Bytes> {
        match self {
            Cache::Memory {
                cache,
                max_entry_size: _,
            } => cache.lock().unwrap().get(path).cloned(),
            Cache::Disk { store } => {
                let log_err = |e: &object_store::Error| log_err(logger, e, false);

                let path = Self::disk_path(path);
                let object = store.get(&path).await.inspect_err(log_err).ok()?;
                let data = object.bytes().await.inspect_err(log_err).ok()?;
                Some(data)
            }
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
                let log_err = |e: &object_store::Error| log_err(logger, e, true);
                let path = Self::disk_path(&path);
                store
                    .put(&path, data.into())
                    .await
                    .inspect_err(log_err)
                    .ok();
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
    pub fn new(client: Arc<dyn IpfsClient>) -> Self {
        let env = &ENV_VARS.mappings;

        let cache = Cache::new(
            env.max_ipfs_cache_size as usize,
            env.max_ipfs_cache_file_size,
            env.ipfs_cache_location.clone(),
        );
        CachingClient { client, cache }
    }

    async fn with_cache<F>(&self, path: &ContentPath, f: F) -> IpfsResult<Bytes>
    where
        F: AsyncFnOnce() -> IpfsResult<Bytes>,
    {
        if let Some(data) = self.cache.find(self.logger(), path).await {
            return Ok(data);
        }

        let data = f().await?;
        self.cache
            .insert(self.logger(), path.clone(), data.clone())
            .await;
        Ok(data)
    }
}

#[async_trait]
impl IpfsClient for CachingClient {
    fn logger(&self) -> &Logger {
        self.client.logger()
    }

    async fn call(self: Arc<Self>, req: IpfsRequest) -> IpfsResult<IpfsResponse> {
        self.client.cheap_clone().call(req).await
    }

    async fn cat(
        self: Arc<Self>,
        path: &ContentPath,
        max_size: usize,
        timeout: Option<Duration>,
        retry_policy: RetryPolicy,
    ) -> IpfsResult<Bytes> {
        self.with_cache(path, async || {
            {
                self.client
                    .cheap_clone()
                    .cat(path, max_size, timeout, retry_policy)
                    .await
            }
        })
        .await
    }

    async fn get_block(
        self: Arc<Self>,
        path: &ContentPath,
        timeout: Option<Duration>,
        retry_policy: RetryPolicy,
    ) -> IpfsResult<Bytes> {
        self.with_cache(path, async || {
            self.client
                .cheap_clone()
                .get_block(path, timeout, retry_policy)
                .await
        })
        .await
    }
}
