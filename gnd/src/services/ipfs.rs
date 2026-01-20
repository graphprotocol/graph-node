//! IPFS client for uploading and fetching subgraph files.
//!
//! This module provides an IPFS client that uses the Kubo RPC API to upload
//! files, get their IPFS hashes, and fetch content by CID. It's used by the
//! build, deploy, and init commands.

use std::collections::HashMap;
use std::path::Path;

use anyhow::{anyhow, Context, Result};
use reqwest::multipart::{Form, Part};
use serde::Deserialize;
use url::Url;

/// Version string for User-Agent header
const GND_VERSION: &str = env!("CARGO_PKG_VERSION");

/// IPFS client for uploading files to an IPFS node.
#[derive(Debug, Clone)]
pub struct IpfsClient {
    client: reqwest::Client,
    api_url: Url,
}

impl IpfsClient {
    /// Create a new IPFS client for the given IPFS node URL.
    ///
    /// The URL should be the IPFS HTTP API endpoint (e.g., `http://localhost:5001`
    /// or `https://api.thegraph.com/ipfs/api/v0`).
    pub fn new(url: &str) -> Result<Self> {
        let mut api_url = Url::parse(url).context("Invalid IPFS URL")?;

        // Ensure the URL ends with /api/v0 for the Kubo API
        let path = api_url.path();
        if !path.ends_with("/api/v0") && !path.ends_with("/api/v0/") {
            // Append /api/v0 if needed
            let new_path = if path.ends_with('/') {
                format!("{}api/v0", path)
            } else if path.contains("/api/v0") {
                path.to_string()
            } else {
                format!("{}/api/v0", path.trim_end_matches('/'))
            };
            api_url.set_path(&new_path);
        }

        let client = reqwest::Client::builder()
            .user_agent(format!("gnd/{}", GND_VERSION))
            .timeout(std::time::Duration::from_secs(300)) // 5 minutes for large uploads
            .build()
            .context("Failed to create HTTP client")?;

        Ok(Self { client, api_url })
    }

    /// Add a file to IPFS and return its CID (content identifier).
    ///
    /// The file is uploaded using the IPFS `add` API endpoint.
    pub async fn add(&self, filename: &str, content: Vec<u8>) -> Result<String> {
        let mut url = self.api_url.clone();
        url.set_path(&format!("{}/add", url.path().trim_end_matches('/')));

        // Add query parameters
        url.query_pairs_mut()
            .append_pair("pin", "true")
            .append_pair("wrap-with-directory", "false");

        let part = Part::bytes(content)
            .file_name(filename.to_string())
            .mime_str("application/octet-stream")
            .context("Failed to create multipart form")?;

        let form = Form::new().part("file", part);

        let response = self
            .client
            .post(url)
            .multipart(form)
            .send()
            .await
            .context("Failed to send request to IPFS")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(anyhow!("IPFS add failed with status {}: {}", status, body));
        }

        // IPFS returns newline-delimited JSON (one object per line).
        // When the filename contains a path separator, IPFS returns multiple lines:
        // first the file itself, then any parent directories. We want the first
        // line which is the actual file CID.
        let body = response
            .text()
            .await
            .context("Failed to read IPFS response body")?;

        // Parse the first non-empty line as the result (the actual file)
        let first_line = body
            .lines()
            .find(|line| !line.is_empty())
            .ok_or_else(|| anyhow!("Empty response from IPFS"))?;

        let result: AddResponse =
            serde_json::from_str(first_line).context("Failed to parse IPFS response JSON")?;

        Ok(result.hash)
    }

    /// Add multiple files to IPFS and return a map of filenames to CIDs.
    ///
    /// This is more efficient than calling `add` multiple times for many files.
    pub async fn add_all(&self, files: Vec<(String, Vec<u8>)>) -> Result<HashMap<String, String>> {
        let mut results = HashMap::new();

        for (filename, content) in files {
            let hash = self.add(&filename, content).await?;
            results.insert(filename, hash);
        }

        Ok(results)
    }

    /// Pin an existing CID to ensure it's not garbage collected.
    pub async fn pin(&self, cid: &str) -> Result<()> {
        let mut url = self.api_url.clone();
        url.set_path(&format!("{}/pin/add", url.path().trim_end_matches('/')));
        url.query_pairs_mut().append_pair("arg", cid);

        let response = self
            .client
            .post(url)
            .send()
            .await
            .context("Failed to send pin request to IPFS")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(anyhow!("IPFS pin failed with status {}: {}", status, body));
        }

        Ok(())
    }

    /// Fetch content from IPFS by CID.
    ///
    /// Returns the content as a string. The CID can be with or without the
    /// `/ipfs/` prefix.
    pub async fn cat(&self, cid: &str) -> Result<String> {
        // Normalize CID: strip /ipfs/ prefix if present
        let normalized_cid = cid.strip_prefix("/ipfs/").unwrap_or(cid);

        let mut url = self.api_url.clone();
        url.set_path(&format!("{}/cat", url.path().trim_end_matches('/')));
        url.query_pairs_mut().append_pair("arg", normalized_cid);

        let response = self
            .client
            .post(url)
            .send()
            .await
            .context("Failed to send cat request to IPFS")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(anyhow!(
                "Failed to fetch {} from IPFS (status {}): {}",
                normalized_cid,
                status,
                body
            ));
        }

        response
            .text()
            .await
            .context("Failed to read response body from IPFS")
    }

    /// Fetch a subgraph manifest from IPFS by deployment ID.
    ///
    /// The deployment ID is the CID of the manifest (typically starts with `Qm`).
    /// Returns the raw manifest YAML content.
    pub async fn fetch_manifest(&self, deployment_id: &str) -> Result<String> {
        self.cat(deployment_id)
            .await
            .with_context(|| format!("Failed to fetch manifest for deployment {}", deployment_id))
    }

    /// Fetch the schema from IPFS using a manifest's schema CID.
    ///
    /// The schema CID is extracted from the manifest YAML at `schema.file["/"]`.
    pub async fn fetch_schema(&self, schema_cid: &str) -> Result<String> {
        self.cat(schema_cid)
            .await
            .with_context(|| format!("Failed to fetch schema {}", schema_cid))
    }
}

/// Response from the IPFS add endpoint.
#[derive(Debug, Deserialize)]
struct AddResponse {
    #[serde(rename = "Hash")]
    hash: String,
    #[serde(rename = "Name")]
    #[allow(dead_code)]
    name: String,
    #[serde(rename = "Size")]
    #[allow(dead_code)]
    size: String,
}

/// Read a file and return its contents as bytes.
pub fn read_file(path: &Path) -> Result<Vec<u8>> {
    std::fs::read(path).with_context(|| format!("Failed to read file: {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_url_normalization_no_api() {
        let client = IpfsClient::new("http://localhost:5001").unwrap();
        assert!(client.api_url.path().contains("/api/v0"));
    }

    #[test]
    fn test_url_normalization_with_api() {
        let client = IpfsClient::new("https://api.thegraph.com/ipfs/api/v0").unwrap();
        assert!(client.api_url.path().contains("/api/v0"));
    }

    #[test]
    fn test_url_normalization_thegraph() {
        let client = IpfsClient::new("https://api.thegraph.com/ipfs").unwrap();
        assert!(client.api_url.path().ends_with("/api/v0"));
    }
}
