pub use failure::Error;
use failure::Fail;

static BASE_URL: &'static str = "https://api.pinata.cloud";

/// All possible error returned from this SDK defined as variants of this enum.
///
/// This also derives the failure::Fail trait, so it should be easier to handle and extend
/// in clients that also support this failure crate.
#[derive(Debug, Fail)]
pub enum ApiError {
    /// Thrown when api_key passed to the [PinataApi](struct.PinataApi.html) is blank.
    #[fail(display = "Invalid api_key")]
    InvalidApiKey(),
    /// Throw when secret_api_key passed to the `PinataApi` is blank.
    #[fail(display = "Invalid secret_api_key")]
    InvalidSecretApiKey(),
    /// A generic error with message on a possible failure while interacting with the api
    #[fail(display = "Error: {}", _0)]
    GenericError(String),
}

impl From<reqwest::Error> for ApiError {
    fn from(req_err: reqwest::Error) -> ApiError {
        ApiError::GenericError(format!("{}", req_err))
    }
}

impl From<std::io::Error> for ApiError {
    fn from(io_err: std::io::Error) -> ApiError {
        ApiError::GenericError(format!("{}", io_err))
    }
}

impl From<std::path::StripPrefixError> for ApiError {
    fn from(io_err: std::path::StripPrefixError) -> ApiError {
        ApiError::GenericError(format!("{}", io_err))
    }
}

/// Checks to ensure keys are not empty
pub fn validate_keys(api_key: &str, secret_api_key: &str) -> Result<(), Error> {
    if api_key.is_empty() {
        Err(ApiError::InvalidApiKey())?
    }

    if secret_api_key.is_empty() {
        Err(ApiError::InvalidSecretApiKey())?
    }

    Ok(())
}

pub fn api_url(path: &str) -> String {
    format!("{}{}", BASE_URL, path)
}
