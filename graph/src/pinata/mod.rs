pub mod api;
pub mod utils;

use failure::Error;
use reqwest::{
    header::HeaderMap,
    multipart::{Form, Part},
    Client, ClientBuilder, Response,
};
use serde::{de::DeserializeOwned, Serialize};
use std::collections::HashMap;

use self::{
    api::{
        data::{
            HashPinPolicy, PinByJson, PinJobs, PinJobsFilter, PinList, PinListFilter, PinnedObject,
            TotalPinnedData,
        },
        internal::PinataApiError,
        metadata::{ChangePinMetadata, MetadataValue, PinMetadata},
    },
    utils::{api_url, validate_keys, ApiError},
};

pub struct PinataApi {
    client: Client,
}

impl PinataApi {
    /// Creates a new instance of PinataApi using the provided keys.
    /// This function panics if api_key or secret_api_key's are empty/blank
    pub fn new<S: Into<String>>(api_key: S, secret_api_key: S) -> Result<PinataApi, Error> {
        let owned_key = api_key.into();
        let owned_secret = secret_api_key.into();

        validate_keys(&owned_key, &owned_secret)?;

        let mut default_headers = HeaderMap::new();
        default_headers.insert("pinata_api_key", (&owned_key).parse().unwrap());
        default_headers.insert("pinata_secret_api_key", (&owned_secret).parse().unwrap());

        let client = ClientBuilder::new()
            .default_headers(default_headers)
            .build()?;

        Ok(PinataApi { client })
    }

    /// Test if your credentials are corrects. It returns an error if credentials are not correct
    pub async fn test_authentication(&self) -> Result<(), ApiError> {
        let response = self
            .client
            .get(&api_url("/data/testAuthentication"))
            .send()
            .await?;

        self.parse_ok_result(response).await
    }

    /// Change the pin policy for an individual piece of content.
    ///
    /// Changes made via this function only affect the content for the hash passed in. They do not affect a user's account level pin policy.
    ///
    /// To read more about pin policies, please check out the [Regions and Replications](https://pinata.cloud/documentation#RegionsAndReplications) documentation
    pub async fn set_hash_pin_policy(&self, policy: HashPinPolicy) -> Result<(), ApiError> {
        let response = self
            .client
            .put(&api_url("/pinning/hashPinPolicy"))
            .json(&policy)
            .send()
            .await?;

        self.parse_ok_result(response).await
    }

    /// Retrieve a list of all the pins that are currently in the pin queue for your user
    pub async fn get_pin_jobs(&self, filters: PinJobsFilter) -> Result<PinJobs, ApiError> {
        let response = self
            .client
            .get(&api_url("/pinning/pinJobs"))
            .query(&filters)
            .send()
            .await?;

        self.parse_result(response).await
    }

    /// Pin any JSON serializable object to Pinata IPFS nodes.
    pub async fn pin_json<S>(&self, pin_data: PinByJson<S>) -> Result<PinnedObject, ApiError>
    where
        S: Serialize,
    {
        let response = self
            .client
            .post(&api_url("/pinning/pinJSONToIPFS"))
            .json(&pin_data)
            .send()
            .await?;

        self.parse_result(response).await
    }

    pub async fn pin_file_content(
        &self,
        data: Vec<u8>,
        mime: String,
    ) -> Result<PinnedObject, ApiError> {
        let mut form = Form::new();
        // Unfold<(Vec<u8>, usize), impl Stream<Item = Result<impl AsRef<[u8]> + Send, reqwest::Error>>>

        let mut keyvalues: HashMap<String, MetadataValue> = HashMap::new();
        keyvalues.insert(
            "Content-Type".to_string(),
            MetadataValue::String(mime.to_string()),
        );
        let metadata = PinMetadata {
            keyvalues,
            name: Some("file".to_string()),
        };
        form = form.text("pinataMetadata", serde_json::to_string(&metadata).unwrap());

        // Convert the stream into a reqwest Body
        let part = Part::bytes(data).file_name("file".to_string());
        form = form.part("file", part);

        let response = self
            .client
            .post(&api_url("/pinning/pinFileToIPFS"))
            .multipart(form)
            .send()
            .await?;

        self.parse_result(response).await
    }

    /// Unpin content previously uploaded to the Pinata's IPFS nodes.
    pub async fn unpin(&self, hash: &str) -> Result<(), ApiError> {
        let response = self
            .client
            .delete(&api_url(&format!("/pinning/unpin/{}", hash)))
            .send()
            .await?;

        self.parse_ok_result(response).await
    }

    /// Change name and custom key values associated for a piece of content stored on Pinata.
    pub async fn change_hash_metadata(&self, change: ChangePinMetadata) -> Result<(), ApiError> {
        let response = self
            .client
            .put(&api_url("/pinning/hashMetadata"))
            .json(&change)
            .send()
            .await?;

        self.parse_ok_result(response).await
    }

    /// This endpoint returns the total combined size for all content that you've pinned through Pinata
    pub async fn get_total_user_pinned_data(&self) -> Result<TotalPinnedData, ApiError> {
        let response = self
            .client
            .get(&api_url("/data/userPinnedDataTotal"))
            .send()
            .await?;

        self.parse_result(response).await
    }

    /// This returns data on what content the sender has pinned to IPFS from pinata
    ///
    /// The purpose of this endpoint is to provide insight into what is being pinned, and how
    /// long it has been pinned. The results of this call can be filtered using [PinListFilter](struct.PinListFilter.html).
    pub async fn get_pin_list(&self, filters: PinListFilter) -> Result<PinList, ApiError> {
        let response = self
            .client
            .get(&api_url("/data/pinList"))
            .query(&filters)
            .send()
            .await?;

        self.parse_result(response).await
    }

    async fn parse_result<R>(&self, response: Response) -> Result<R, ApiError>
    where
        R: DeserializeOwned,
    {
        if response.status().is_success() {
            let result = response.json::<R>().await?;
            Ok(result)
        } else {
            let error = response.json::<PinataApiError>().await?;
            Err(ApiError::GenericError(error.message()))
        }
    }

    async fn parse_ok_result(&self, response: Response) -> Result<(), ApiError> {
        if response.status().is_success() {
            Ok(())
        } else {
            let error = response.json::<PinataApiError>().await?;
            Err(ApiError::GenericError(error.message()))
        }
    }
}
