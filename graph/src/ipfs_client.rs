use anyhow::anyhow;
use anyhow::Error;
use bytes::Bytes;
use bytes::BytesMut;
use cid::Cid;
use futures03::stream::TryStreamExt as _;
use futures03::Stream;
use http::header::CONTENT_LENGTH;
use http::Uri;
use reqwest::multipart;
use serde::Deserialize;
use std::fmt::Display;
use std::time::Duration;
use std::{str::FromStr, sync::Arc};

use crate::derive::CheapClone;

#[derive(Debug, thiserror::Error)]
pub enum IpfsError {
    #[error("Request error: {0}")]
    Request(#[from] reqwest::Error),
    #[error("IPFS file {0} is too large. It can be at most {1} bytes")]
    FileTooLarge(String, usize),
}

impl IpfsError {
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

/// Represents a file on Ipfs. This file can be the CID or a path within a folder CID.
/// The path cannot have a prefix (ie CID/hello.json would be cid: CID path: "hello.json")
#[derive(Debug, Clone, Default, Eq, PartialEq, Hash)]
pub struct CidFile {
    pub cid: Cid,
    pub path: Option<String>,
}

impl Display for CidFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let str = match self.path {
            Some(ref f) => format!("{}/{}", self.cid, f),
            None => self.cid.to_string(),
        };
        f.write_str(&str)
    }
}

impl CidFile {
    pub fn to_bytes(&self) -> Vec<u8> {
        self.to_string().as_bytes().to_vec()
    }
}

impl TryFrom<crate::data::store::scalar::Bytes> for CidFile {
    type Error = anyhow::Error;

    fn try_from(value: crate::data::store::scalar::Bytes) -> Result<Self, Self::Error> {
        let str = String::from_utf8(value.to_vec())?;

        Self::from_str(&str)
    }
}

/// The string should not have a prefix and only one slash after the CID is removed, everything
/// else is considered a file path. If this is malformed, it will fail to find the file.
impl FromStr for CidFile {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            return Err(anyhow!("cid can't be empty"));
        }

        let cid_str: String = s.chars().take_while(|c| *c != '/').collect();
        let cid = Cid::from_str(&cid_str)?;

        // if cid was the only content or if it's just slash terminated.
        if cid_str.len() == s.len() || s.len() + 1 == cid_str.len() {
            return Ok(CidFile { cid, path: None });
        }

        let file: String = s[cid_str.len() + 1..].to_string();
        let path = if file.is_empty() { None } else { Some(file) };

        Ok(CidFile { cid, path })
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct AddResponse {
    pub name: String,
    pub hash: String,
    pub size: String,
}

/// Reference type, clones will share the connection pool.
#[derive(Clone, CheapClone)]
pub struct IpfsClient {
    base: Arc<Uri>,
    // reqwest::Client doesn't need to be `Arc` because it has one internally
    // already.
    client: reqwest::Client,
}

impl IpfsClient {
    pub fn new(base: &str) -> Result<Self, Error> {
        Ok(IpfsClient {
            client: reqwest::Client::new(),
            base: Arc::new(Uri::from_str(base)?),
        })
    }

    pub fn localhost() -> Self {
        IpfsClient {
            client: reqwest::Client::new(),
            base: Arc::new(Uri::from_str("http://localhost:5001").unwrap()),
        }
    }

    /// To check the existence of a cid, we do a cat of a single byte.
    pub async fn exists(&self, cid: &str, timeout: Option<Duration>) -> Result<(), IpfsError> {
        self.call(self.cat_url("cat", cid, Some(1)), None, timeout)
            .await?;
        Ok(())
    }

    pub async fn cat_all(
        &self,
        cid: &str,
        timeout: Option<Duration>,
        max_file_size: usize,
    ) -> Result<Bytes, IpfsError> {
        let byte_stream = self.cat_stream(cid, timeout).await?;
        let bytes = byte_stream
            .err_into()
            .try_fold(BytesMut::new(), |mut acc, chunk| async move {
                acc.extend_from_slice(&chunk);

                // Check size limit
                if acc.len() > max_file_size {
                    return Err(IpfsError::FileTooLarge(cid.to_string(), max_file_size));
                }

                Ok(acc)
            })
            .await?;
        Ok(bytes.into())
    }
    pub async fn cat_stream(
        &self,
        cid: &str,
        timeout: Option<Duration>,
    ) -> Result<impl Stream<Item = Result<Bytes, reqwest::Error>> + 'static, reqwest::Error> {
        Ok(self
            .call(self.cat_url("cat", cid, None), None, timeout)
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

    fn cat_url(&self, route: &str, arg: &str, length: Option<u64>) -> String {
        // URL security: We control the base and the route, user-supplied input goes only into the
        // query parameters.
        let mut url = format!("{}api/v0/{}?arg={}", self.base, route, arg);
        if let Some(length) = length {
            url.push_str(&format!("&length={}", length));
        }
        url
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

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use anyhow::anyhow;
    use cid::Cid;

    use crate::ipfs_client::CidFile;

    #[test]
    fn test_cid_parsing() {
        let cid_str = "bafyreibjo4xmgaevkgud7mbifn3dzp4v4lyaui4yvqp3f2bqwtxcjrdqg4";
        let cid = Cid::from_str(cid_str).unwrap();

        struct Case<'a> {
            name: &'a str,
            input: String,
            path: String,
            expected: Result<CidFile, anyhow::Error>,
        }

        let cases = vec![
            Case {
                name: "correct no slashes, no file",
                input: cid_str.to_string(),
                path: cid_str.to_string(),
                expected: Ok(CidFile { cid, path: None }),
            },
            Case {
                name: "correct with file path",
                input: format!("{}/file.json", cid),
                path: format!("{}/file.json", cid_str),
                expected: Ok(CidFile {
                    cid,
                    path: Some("file.json".into()),
                }),
            },
            Case {
                name: "correct cid with trailing slash",
                input: format!("{}/", cid),
                path: format!("{}", cid),
                expected: Ok(CidFile { cid, path: None }),
            },
            Case {
                name: "incorrect, empty",
                input: "".to_string(),
                path: "".to_string(),
                expected: Err(anyhow!("cid can't be empty")),
            },
            Case {
                name: "correct, two slahes",
                input: format!("{}//", cid),
                path: format!("{}//", cid),
                expected: Ok(CidFile {
                    cid,
                    path: Some("/".into()),
                }),
            },
            Case {
                name: "incorrect, leading slahes",
                input: format!("/ipfs/{}/file.json", cid),
                path: "".to_string(),
                expected: Err(anyhow!("Input too short")),
            },
            Case {
                name: "correct syntax, invalid CID",
                input: "notacid/file.json".to_string(),
                path: "".to_string(),
                expected: Err(anyhow!("Failed to parse multihash")),
            },
        ];

        for case in cases {
            let f = CidFile::from_str(&case.input);

            match case.expected {
                Ok(cid_file) => {
                    assert!(f.is_ok(), "case: {}", case.name);
                    let f = f.unwrap();
                    assert_eq!(f, cid_file, "case: {}", case.name);
                    assert_eq!(f.to_string(), case.path, "case: {}", case.name);
                }
                Err(err) => assert_eq!(
                    f.unwrap_err().to_string(),
                    err.to_string(),
                    "case: {}",
                    case.name
                ),
            }
        }
    }
}
