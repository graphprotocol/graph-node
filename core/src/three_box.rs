use graph::components::three_box::ThreeBoxAdapter as ThreeBoxAdapterTrait;
use graph::prelude::*;
use graph::url::Url;

pub struct ThreeBoxAdapter {
    profile_endpoint: Url,
    http_client: reqwest::Client,
}

impl ThreeBoxAdapter {
    /// Panics if `profile_endpoint` is not a valid URL.
    pub fn new(mut profile_endpoint: String) -> Self {
        // Make sure the endpoint has a trailing slash so `Url::join` works.
        if !profile_endpoint.ends_with('/') {
            profile_endpoint.push('/')
        }

        ThreeBoxAdapter {
            profile_endpoint: Url::parse(&profile_endpoint).expect("Invalid 3box profile URL"),
            http_client: reqwest::Client::new(),
        }
    }
}

#[async_trait]
impl ThreeBoxAdapterTrait for ThreeBoxAdapter {
    // See https://github.com/3box/3box-js/blob/510137adbdf3ef4e240d9a7789946e967a19ff30/src/api.js#L160
    async fn profile(
        &self,
        address: &str,
    ) -> Result<serde_json::Map<String, serde_json::Value>, Error> {
        let mut url = self.profile_endpoint.join("/profile").unwrap();
        if address.starts_with("did") {
            // It's a DID.
            url.set_query(Some(&format!("did={}", address)));
        } else {
            // Assume it's an Ethereum address.
            url.set_query(Some(&format!("address={}", address)));
        }

        serde_json::from_str(
            &self
                .http_client
                .get(url)
                .timeout(Duration::from_secs(60))
                .send()
                .and_then(|res| async { res.error_for_status() })
                .and_then(|res| res.text())
                .err_into::<Error>()
                .await?,
        )
        .map_err(Into::into)
    }
}
