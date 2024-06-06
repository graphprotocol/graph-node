use super::metadata::{MetadataKeyValues, MetadataValue, PinListMetadata, PinMetadata};
use derive_builder::Builder;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Clone, Debug, Deserialize, Serialize)]
/// All the currently supported regions on Pinata
pub enum Region {
    /// Frankfurt, Germany (max 2 replications)
    FRA1,
    /// New York City, USA (max 2 replications)
    NYC1,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
/// Region and desired replication for that region
pub struct RegionPolicy {
    /// Region Id
    pub id: Region,
    /// Replication count for the region. Maximum of 2 in most regions
    pub desired_replication_count: u8,
}

#[derive(Debug, Deserialize, Serialize)]
/// Pinata Pin Policy Regions
pub struct PinPolicy {
    /// List of regions and their Policy
    pub regions: Vec<RegionPolicy>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
/// Represents a PinPolicy linked to a particular ipfs pinned hash
pub struct HashPinPolicy {
    ipfs_pin_hash: String,
    new_pin_policy: PinPolicy,
}

impl HashPinPolicy {
    /// Create a new HashPinPolicy.
    ///
    /// See the [pinata docs](https://pinata.cloud/documentation#HashPinPolicy) for more information.
    pub fn new<S>(ipfs_pin_hash: S, regions: Vec<RegionPolicy>) -> HashPinPolicy
    where
        S: Into<String>,
    {
        HashPinPolicy {
            ipfs_pin_hash: ipfs_pin_hash.into(),
            new_pin_policy: PinPolicy { regions },
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
/// Status of Jobs
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    /// Pinata is running preliminary validations on your pin request.
    Prechecking,
    /// Pinata is actively searching for your content on the IPFS network.
    Searching,
    /// Pinata has located your content and is now in the process of retrieving it.
    Retrieving,
    /// Pinata wasn't able to find your content after a day of searching the IPFS network.
    Expired,
    /// Pinning this object would put you over the free tier limit. Please add a credit card
    /// to continue.
    OverFreeLimit,
    /// This object is too large of an item to pin. If you're seeing this, please contact pinata
    /// for a more custom solution.
    OverMaxSize,
    /// The object you're attempting to pin isn't readable by IPFS nodes.
    InvalidObject,
    /// You provided a host node that was either invalid or unreachable.
    BadHostNode,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
/// Represents response of a pinByHash request.
pub struct PinByHashResult {
    /// This is Pinata's ID for the pin job.
    pub id: String,
    /// This is the IPFS multi-hash provided to Pinata to pin.
    pub ipfs_hash: String,
    /// Current status of the pin job.
    pub status: JobStatus,
    /// The name of the pin (if provided initially)
    pub name: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
/// Used to add additional options when pinning by hash
pub struct PinOptions {
    /// multiaddresses of nodes your content is already stored on
    pub host_nodes: Option<Vec<String>>,
    /// Custom pin policy for the piece of content being pinned
    pub custom_pin_policy: Option<PinPolicy>,
    /// CID Version IPFS will use when creating a hash for your content
    pub cid_version: Option<u8>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
/// Request object to pin hash of an already existing IPFS hash to pinata.
///
/// ## Example
/// ```
/// # use pinata_sdk::{ApiError, PinataApi, PinByHash};
/// # async fn run() -> Result<(), ApiError> {
/// let api = PinataApi::new("api_key", "secret_api_key").unwrap();
///
/// let result = api.pin_by_hash(PinByHash::new("hash")).await;
///
/// if let Ok(pinning_job) = result {
///   // track result job here
/// }
/// # Ok(())
/// # }
/// ```
pub struct PinByHash {
    hash_to_pin: String,
    pinata_metadata: Option<PinMetadata>,
    pinata_option: Option<PinOptions>,
}

impl PinByHash {
    /// Create a new default PinByHash object with only the hash to pin set.
    ///
    /// To set the pinata metadata and pinata options use the `set_metadata()` and
    /// `set_options()` chainable function to set those values.
    pub fn new<S>(hash: S) -> PinByHash
    where
        S: Into<String>,
    {
        PinByHash {
            hash_to_pin: hash.into(),
            pinata_metadata: None,
            pinata_option: None,
        }
    }

    /// Consumes the current PinByHash and returns a new PinByHash with keyvalues metadata set
    pub fn set_metadata(self, keyvalues: MetadataKeyValues) -> PinByHash {
        PinByHash {
            hash_to_pin: self.hash_to_pin,
            pinata_metadata: Some(PinMetadata {
                keyvalues,
                name: None,
            }),
            pinata_option: self.pinata_option,
        }
    }

    /// Consumes the current PinByHash and returns a new PinByHash with metadata name and keyvalues set
    pub fn set_metadata_with_name<S>(
        self,
        name: S,
        keyvalues: HashMap<String, MetadataValue>,
    ) -> PinByHash
    where
        S: Into<String>,
    {
        PinByHash {
            hash_to_pin: self.hash_to_pin,
            pinata_metadata: Some(PinMetadata {
                keyvalues,
                name: Some(name.into()),
            }),
            pinata_option: self.pinata_option,
        }
    }

    /// Consumes the PinByHash and returns a new PinByHash with pinata options set.
    pub fn set_options(self, options: PinOptions) -> PinByHash {
        PinByHash {
            hash_to_pin: self.hash_to_pin,
            pinata_metadata: self.pinata_metadata,
            pinata_option: Some(options),
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
/// Request object to pin json
///
/// ## Example
/// ```
/// # use pinata_sdk::{ApiError, PinataApi, PinByJson};
/// # use std::collections::HashMap;
/// # async fn run() -> Result<(), ApiError> {
/// let api = PinataApi::new("api_key", "secret_api_key").unwrap();
///
/// let mut json_data = HashMap::new();
/// json_data.insert("name", "user");
///
/// let result = api.pin_json(PinByJson::new(json_data)).await;
///
/// if let Ok(pinned_object) = result {
///   let hash = pinned_object.ipfs_hash;
/// }
/// # Ok(())
/// # }
/// ```
pub struct PinByJson<S: Serialize> {
    pinata_content: S,
    pinata_metadata: Option<PinMetadata>,
    pinata_option: Option<PinOptions>,
}

impl<S> PinByJson<S>
where
    S: Serialize,
{
    /// Create a new default PinByHash object with only the hash to pin set.
    ///
    /// To set the pinata metadata and pinata options use the `set_metadata()` and
    /// `set_options()` chainable function to set those values.
    pub fn new(json_data: S) -> PinByJson<S> {
        PinByJson {
            pinata_content: json_data,
            pinata_metadata: None,
            pinata_option: None,
        }
    }

    /// Consumes the current PinByJson<S> and returns a new PinByJson<S> with keyvalues metadata set
    pub fn set_metadata(mut self, keyvalues: MetadataKeyValues) -> PinByJson<S> {
        self.pinata_metadata = Some(PinMetadata {
            name: None,
            keyvalues,
        });
        self
    }

    /// Consumes the current PinByJson<S> and returns a new PinByJson<S> with keyvalues metadata set
    pub fn set_metadata_with_name<IntoStr>(
        mut self,
        name: IntoStr,
        keyvalues: MetadataKeyValues,
    ) -> PinByJson<S>
    where
        IntoStr: Into<String>,
    {
        self.pinata_metadata = Some(PinMetadata {
            name: Some(name.into()),
            keyvalues,
        });
        self
    }

    /// Consumes the PinByHash and returns a new PinByHash with pinata options set.
    pub fn set_options(mut self, options: PinOptions) -> PinByJson<S> {
        self.pinata_option = Some(options);
        self
    }
}

#[derive(Clone, Serialize)]
/// Sort Direction
pub enum SortDirection {
    /// Sort by ascending dates
    ASC,
    /// Sort by descending dates
    DESC,
}

#[derive(Builder, Clone, Default, Serialize)]
#[builder(setter(into, strip_option, prefix = "set"), default)]
/// Filter parameters for fetching PinJobs
///
/// Example of how to use this:
/// ```
/// use pinata_sdk::{PinJobsFilterBuilder, PinJobsFilter, SortDirection, JobStatus};
///
/// let filters: PinJobsFilter = PinJobsFilterBuilder::default()
///   .set_sort(SortDirection::ASC)
///   .set_status(JobStatus::Prechecking)
///   .set_limit(1 as u16)
///   // ...other possible filter set methods
///   .build().unwrap();
/// ```
pub struct PinJobsFilter {
    /// Sort the results by the date added to the pinning queue
    sort: Option<SortDirection>,
    /// Set a status on the PinJobsFilter
    status: Option<JobStatus>,
    /// Set a IPFS pin hash on the PinJobsFilter
    ipfs_pin_hash: Option<String>,
    /// Set limit on the amount of results per page
    limit: Option<u16>,
    /// Set the record offset for records returned. This is how to retrieve additional pages
    offset: Option<u64>,
}

#[derive(Debug, Deserialize)]
/// Pin Job Record
pub struct PinJob {
    /// The id for the pin job record
    pub id: String,
    /// The IPFS mult-hash for the content pinned.
    pub ipfs_pin_hash: String,
    /// The date hash was initially queued. Represented in ISO8601 format
    pub date_queued: String,
    /// The current status for the pin job
    pub status: JobStatus,
    /// Optional name passed for hash
    pub name: Option<String>,
    /// Optional keyvalues metadata passsed for hash
    pub keyvalues: Option<HashMap<String, String>>,
    /// Optional list of host nodes passed for the hash
    pub host_nodes: Option<Vec<String>>,
    /// PinPolicy applied to content once it is found
    pub pin_policy: Option<PinPolicy>,
}

#[derive(Debug, Deserialize)]
/// Represents a list of pin job records for a set of filters.
pub struct PinJobs {
    /// Total number of pin job records that exist for the PinJobsFilter used
    pub count: u64,
    /// Each item in the rows represents a pin job record
    pub rows: Vec<PinJob>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
/// Represents a PinnedObject
pub struct PinnedObject {
    /// IPFS multi-hash provided back for your content
    pub ipfs_hash: String,
    /// This is how large (in bytes) the content you just pinned is
    pub pin_size: u64,
    /// Timestamp for your content pinning in ISO8601 format
    pub timestamp: String,
}

#[derive(Debug, Deserialize)]
/// Results of a call to get total users pinned data
pub struct TotalPinnedData {
    /// The number of pins you currently have pinned with Pinata
    pub pin_count: u128,
    /// The total size of all unique content you have pinned with Pinata (expressed in bytes)
    pub pin_size_total: String,
    /// The total size of all content you have pinned with Pinata. This value is derived by multiplying the size of each piece of unique content by the number of times that content is replicated.
    pub pin_size_with_replications_total: String,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "lowercase")]
/// Status used with [PinListFilterBuilder](struct.PinListFilterBuilder.html)
/// to filter on pin list results.
pub enum PinListFilterStatus {
    /// For both pinned and unpinned records
    All,
    /// For just pinned records (hashes that are currently pinned)
    Pinned,
    /// For just unpinned records (previous hashes that are no longer being pinned on pinata)
    Unpinned,
}

#[derive(Builder, Default, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
#[builder(setter(strip_option, prefix = "set"), default)]
/// Options to filter your pin list based on a number of different options
///
/// Create and set values using [PinListFilterBuilder](struct.PinListFilterBuilder.html).
///
/// ```
/// use pinata_sdk::PinListFilterBuilder;
///
/// let filter = PinListFilterBuilder::default()
///   .set_hash_contains("QmWsZfQw98k9dfG1sDZB3z8YqMtxG9gYCyddgZGWq4w6Z3".to_string())
///   .build()
///   .unwrap();
/// ```
pub struct PinListFilter {
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Filter on alphanumeric characters inside of pin hashes. Hashes which do not include the characters passed in will not be returned
    hash_contains: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Exclude pin records that were pinned before the passed in "pinStart" datetime
    /// (must be in ISO_8601 format)
    pin_start: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// (must be in ISO_8601 format) - Exclude pin records that were pinned after the passed in "pinEnd" datetime
    pin_end: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// (must be in ISO_8601 format) - Exclude pin records that were unpinned before the passed in "unpinStart" datetime
    unpin_start: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// (must be in ISO_8601 format) - Exclude pin records that were unpinned after the passed in "unpinEnd" datetime
    unpin_end: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// The minimum byte size that pin record you're looking for can have
    pin_size_min: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// The maximum byte size that pin record you're looking for can have
    pin_size_max: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// The status of pin lists results
    status: Option<PinListFilterStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Filter on metadata name or metadata keyvalues.
    /// If specifying a `metadata[keyvalues]` filter, you need to ensure that you encode the values as the recommended
    /// JSON accordingly. See the pinata docs [here](https://pinata.cloud/documentation#PinList) under the 'Metadata Querying'
    /// section for more details.
    metadata: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// This sets the amount of records that will be returned per API response. (Max 1000)
    page_limit: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// This tells the API how far to offset the record responses. For example, if there's 30 records that match your query, and you passed in a pageLimit of 10, providing a pageOffset of 10 would return records 11-20
    page_offset: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
/// RegionPolicy active on the PinListItem
pub struct PinListItemRegionPolicy {
    /// Region this content is expected to be pinned in
    pub region_id: Region,
    /// The number of replications desired in this region
    pub desired_replication_count: u8,
    /// The number of times this content has been replicated so far
    pub current_replication_count: u8,
}

#[derive(Debug, Deserialize)]
/// A pinned item gotten from get PinList request
///
/// This is usually as part of the PinList struct which is gotten as response
pub struct PinListItem {
    /// The id of this pin item record
    pub id: String,
    /// The IPFS multihash for this items content
    pub ipfs_pin_hash: String,
    /// Size in bytes of the content pinned
    pub size: usize,
    /// Users Pinata id
    pub user_id: String,
    /// ISO 8601 timestamp for when this content was pinned.
    pub date_pinned: String,
    /// ISO 8601 timestamp for when this content was unpinned.
    ///
    /// Is None for content that is not yet unpinned
    pub data_unpinned: Option<String>,
    /// Metadata of the original uploaded files
    pub metadata: PinListMetadata,
    /// Region Policy set on the item
    pub regions: Vec<PinListItemRegionPolicy>,
}

#[derive(Debug, Deserialize)]
/// Result of request to get pinList
pub struct PinList {
    /// Total number of pin records that exist for the query filters passed
    pub count: u128,
    /// List of pinned item in the result set
    pub rows: Vec<PinListItem>,
}
