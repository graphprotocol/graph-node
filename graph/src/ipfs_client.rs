use crate::prelude::CheapClone;
use anyhow::Error;
use bytes::Bytes;
use futures03::Stream;
use http::header::CONTENT_LENGTH;
use http::Uri;
use reqwest::multipart;
use serde::Deserialize;
use std::time::Duration;
use std::{str::FromStr, sync::Arc};

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum StatApi {
    Block,
    Files,
}

impl StatApi {
    fn route(&self) -> &'static str {
        match self {
            Self::Block => "block",
            Self::Files => "files",
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct BlockStatResponse {
    size: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct FilesStatResponse {
    cumulative_size: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct AddResponse {
    pub name: String,
    pub hash: String,
    pub size: String,
}

#[derive(Clone)]
pub struct IpfsClient {
    base: Arc<Uri>,
    client: Arc<reqwest::Client>,
}

impl CheapClone for IpfsClient {
    fn cheap_clone(&self) -> Self {
        IpfsClient {
            base: self.base.cheap_clone(),
            client: self.client.cheap_clone(),
        }
    }
}

impl IpfsClient {
    pub fn new(base: &str) -> Result<Self, Error> {
        Ok(IpfsClient {
            client: Arc::new(reqwest::Client::new()),
            base: Arc::new(Uri::from_str(base)?),
        })
    }

    pub fn localhost() -> Self {
        IpfsClient {
            client: Arc::new(reqwest::Client::new()),
            base: Arc::new(Uri::from_str("http://localhost:5001").unwrap()),
        }
    }

    /// Calls stat for the given API route, and returns the total size of the object.
    pub async fn stat_size(
        &self,
        api: StatApi,
        mut cid: String,
        timeout: Duration,
    ) -> Result<u64, reqwest::Error> {
        let route = format!("{}/stat", api.route());
        if api == StatApi::Files {
            // files/stat requires a leading `/ipfs/`.
            cid = format!("/ipfs/{}", cid);
        }
        let url = self.url(&route, cid);
        let res = self.call(url, None, Some(timeout)).await?;
        match api {
            StatApi::Files => Ok(res.json::<FilesStatResponse>().await?.cumulative_size),
            StatApi::Block => Ok(res.json::<BlockStatResponse>().await?.size),
        }
    }

    /// Download the entire contents.
    pub async fn cat_all(&self, cid: String, timeout: Duration) -> Result<Bytes, reqwest::Error> {
        self.call(self.url("cat", cid), None, Some(timeout))
            .await?
            .bytes()
            .await
    }

    pub async fn cat(
        &self,
        cid: String,
    ) -> Result<impl Stream<Item = Result<Bytes, reqwest::Error>>, reqwest::Error> {
        Ok(self
            .call(self.url("cat", cid), None, None)
            .await?
            .bytes_stream())
    }

    pub async fn get_block(&self, cid: String) -> Result<Bytes, reqwest::Error> {
        let form = multipart::Form::new().part("arg", multipart::Part::text(cid));
        self.call(format!("{}api/v0/block/get", self.base), Some(form), None)
            .await?
            .bytes()
            .await
    }

    pub async fn test(&self) -> Result<(), reqwest::Error> {
        self.call(format!("{}api/v0/version", self.base), None, None)
            .await
            .map(|_| ())
    }

    pub async fn add(&self, data: Vec<u8>) -> Result<AddResponse, reqwest::Error> {
        let form = multipart::Form::new().part("path", multipart::Part::bytes(data));

        self.call(format!("{}api/v0/add", self.base), Some(form), None)
            .await?
            .json()
            .await
    }

    fn url(&self, route: &str, arg: String) -> String {
        // URL security: We control the base and the route, user-supplied input goes only into the
        // query parameters.
        format!("{}api/v0/{}?arg={}", self.base, route, arg)
    }

    async fn call(
        &self,
        url: String,
        form: Option<multipart::Form>,
        timeout: Option<Duration>,
    ) -> Result<reqwest::Response, reqwest::Error> {
        let mut req = self.client.post(&url);
        if let Some(form) = form {
            req = req.multipart(form);
        } else {
            // Some servers require `content-length` even for an empty body.
            req = req.header(CONTENT_LENGTH, 0);
        }

        if let Some(timeout) = timeout {
            req = req.timeout(timeout)
        }

        req.send()
            .await
            .map(|res| res.error_for_status())
            .and_then(|x| x)
    }
}
