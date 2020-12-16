use graph::bytes::Bytes;
use graph::components::arweave::ArweaveAdapter as ArweaveAdapterTrait;
use graph::prelude::*;
use graph::url::Url;
use reqwest::header;

pub struct ArweaveAdapter {
    endpoint: Url,
    http_client: reqwest::Client,
}

impl ArweaveAdapter {
    /// Panics if `endpoint` is not a valid URL.
    pub fn new(mut endpoint: String) -> Self {
        // Make sure the endpoint has a trailing slash so `Url::join` works.
        if !endpoint.ends_with('/') {
            endpoint.push('/')
        }

        ArweaveAdapter {
            endpoint: Url::parse(&endpoint).expect("Invalid Arweave URL"),
            http_client: reqwest::Client::new(),
        }
    }
}

#[async_trait]
impl ArweaveAdapterTrait for ArweaveAdapter {
    async fn tx_data(&self, tx_id: &str) -> Result<Bytes, Error> {
        // Check that the user input is encoded in base64url, and is therefore safe to interpolate.
        if !tx_id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
        {
            return Err(anyhow::anyhow!(
                "Invalid Arweave transaction id: `{}`",
                tx_id
            ));
        }

        self.http_client
            .get(self.endpoint.join(&format!("tx/{}/data.", tx_id)).unwrap())
            .header(
                header::CONTENT_TYPE,
                header::HeaderValue::from_static("application/octet-stream"),
            )
            .timeout(Duration::from_secs(60))
            .send()
            .and_then(|res| async { res.error_for_status() })
            .and_then(|res| res.bytes())
            .err_into()
            .await
    }
}
