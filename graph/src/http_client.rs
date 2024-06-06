use std::sync::{Arc, Mutex};

use bytes::Bytes;
use futures03::Stream;
use lru_time_cache::LruCache;
use reqwest::Client;
use slog::{debug, Logger};

use crate::cheap_clone::CheapClone;
// use crate::data_source::offchain::Base64;
use crate::prelude::*;
use std::fmt::Debug;

#[derive(Clone)]
pub struct HttpClient {
    client: Client,
    logger: Logger,
    cache: Arc<Mutex<LruCache<String, Vec<u8>>>>,
    timeout: Duration,
    retry: bool,
}

impl Debug for HttpClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpClient").finish()
    }
}

#[derive(Debug, Clone)]
pub enum FileSizeLimit {
    Unlimited,
    MaxBytes(u64),
}

impl CheapClone for FileSizeLimit {
    fn cheap_clone(&self) -> Self {
        match self {
            Self::Unlimited => Self::Unlimited,
            Self::MaxBytes(max) => Self::MaxBytes(*max),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum HttpClientError {
    #[error("Request error: {0}")]
    Request(#[from] reqwest::Error),
    #[error("HTTP file {0} is too large. It can be at most {1} bytes")]
    FileTooLarge(u64, u64),
    #[error("HTTP doesn't return content-length")]
    UnableToCheckFileSize,
}

impl HttpClientError {
    pub fn is_timeout(&self) -> bool {
        match self {
            Self::Request(e) => e.is_timeout(),
            _ => false,
        }
    }

    /// Is this error from an HTTP status code?
    pub fn is_status(&self) -> bool {
        match self {
            Self::Request(e) => e.is_status(),
            _ => false,
        }
    }

    pub fn status(&self) -> Option<http::StatusCode> {
        match self {
            Self::Request(e) => e.status(),
            _ => None,
        }
    }
}

impl Default for HttpClient {
    fn default() -> Self {
        Self {
            client: Client::default(),
            logger: Logger::root(slog::Discard, o!()),
            cache: Arc::new(Mutex::new(LruCache::with_capacity(100))),
            timeout: Duration::from_secs(30),
            retry: false,
        }
    }
}

impl HttpClient {
    pub fn new(logger: Logger, timeout: &Duration, retry: bool) -> Self {
        Self {
            logger,
            client: Client::default(),
            cache: Arc::new(Mutex::new(LruCache::with_capacity(100))),
            timeout: timeout.to_owned(),
            retry,
        }
    }

    pub async fn get(&self, file: &Link) -> Result<Vec<u8>, HttpClientError> {
        self.get_with_limit(file, &FileSizeLimit::Unlimited).await
    }

    pub async fn get_stream(
        &self,
        file: &Link,
    ) -> Result<impl Stream<Item = Result<Bytes, reqwest::Error>> + 'static, HttpClientError> {
        let url = file.link.clone();
        let rsp = self
            .client
            .get(&url)
            .timeout(self.timeout)
            .send()
            .await
            .map(|res| res.error_for_status())
            .and_then(|x| x)
            .map_err(HttpClientError::from)?;

        debug!(self.logger, "Got http file {url}");

        Ok(rsp.bytes_stream())
    }

    pub async fn get_with_limit(
        &self,
        file: &Link,
        limit: &FileSizeLimit,
    ) -> Result<Vec<u8>, HttpClientError> {
        let url = file.link.clone();
        let rsp = self
            .client
            .get(&url)
            .timeout(self.timeout)
            .send()
            .await
            .map_err(HttpClientError::from)?;

        match (&limit, rsp.content_length()) {
            (_, None) => return Err(HttpClientError::UnableToCheckFileSize),
            (FileSizeLimit::MaxBytes(max), Some(cl)) if cl > *max => {
                return Err(HttpClientError::FileTooLarge(cl, *max))
            }
            _ => {}
        };

        debug!(self.logger, "Got arweave file {url}");

        rsp.bytes()
            .await
            .map(|b| b.into())
            .map_err(HttpClientError::from)
    }
}

impl CheapClone for HttpClient {
    fn cheap_clone(&self) -> Self {
        Self {
            logger: self.logger.clone(),
            client: self.client.clone(),
            cache: self.cache.clone(),
            timeout: self.timeout,
            retry: self.retry,
        }
    }
}
