pub mod errors {
    pub use failure::Error;
    use failure::Fail;

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

    impl From<walkdir::Error> for ApiError {
        fn from(io_err: walkdir::Error) -> ApiError {
            ApiError::GenericError(format!("{}", io_err))
        }
    }

    impl From<std::path::StripPrefixError> for ApiError {
        fn from(io_err: std::path::StripPrefixError) -> ApiError {
            ApiError::GenericError(format!("{}", io_err))
        }
    }
}
