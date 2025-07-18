use anyhow::anyhow;
use cid::Cid;
use url::Url;

use crate::ipfs::IpfsError;
use crate::ipfs::IpfsResult;

/// Represents a path to some data on IPFS.
#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ContentPath {
    cid: Cid,
    path: Option<String>,
}

impl ContentPath {
    /// Creates a new [ContentPath] from the specified input.
    ///
    /// Supports the following formats:
    /// - <CID>[/<path>]
    /// - /ipfs/<CID>[/<path>]
    /// - ipfs://<CID>[/<path>]
    /// - http[s]://.../ipfs/<CID>[/<path>]
    /// - http[s]://.../api/v0/cat?arg=<CID>[/<path>]
    pub fn new(input: impl AsRef<str>) -> IpfsResult<Self> {
        let input = input.as_ref().trim();

        if input.is_empty() {
            return Err(IpfsError::InvalidContentPath {
                input: "".to_string(),
                source: anyhow!("content path is empty"),
            });
        }

        if input.starts_with("http://") || input.starts_with("https://") {
            return Self::parse_from_url(input);
        }

        Self::parse_from_cid_and_path(input)
    }

    fn parse_from_url(input: &str) -> IpfsResult<Self> {
        let url = Url::parse(input).map_err(|_err| IpfsError::InvalidContentPath {
            input: input.to_string(),
            source: anyhow!("input is not a valid URL"),
        })?;

        if let Some((_, x)) = url.query_pairs().find(|(key, _)| key == "arg") {
            return Self::parse_from_cid_and_path(&x);
        }

        if let Some((_, x)) = url.path().split_once("/ipfs/") {
            return Self::parse_from_cid_and_path(x);
        }

        Self::parse_from_cid_and_path(url.path())
    }

    fn parse_from_cid_and_path(mut input: &str) -> IpfsResult<Self> {
        input = input.trim_matches('/');

        for prefix in ["ipfs/", "ipfs://"] {
            if let Some(input_without_prefix) = input.strip_prefix(prefix) {
                input = input_without_prefix
            }
        }

        let (cid, path) = input.split_once('/').unwrap_or((input, ""));

        let cid = cid
            .parse::<Cid>()
            .map_err(|err| IpfsError::InvalidContentPath {
                input: input.to_string(),
                source: anyhow::Error::from(err).context("invalid CID"),
            })?;

        if path.contains('?') {
            return Err(IpfsError::InvalidContentPath {
                input: input.to_string(),
                source: anyhow!("query parameters not allowed"),
            });
        }

        Ok(Self {
            cid,
            path: if path.is_empty() {
                None
            } else {
                Some(path.to_string())
            },
        })
    }

    pub fn cid(&self) -> &Cid {
        &self.cid
    }

    pub fn path(&self) -> Option<&str> {
        self.path.as_deref()
    }
}

impl std::str::FromStr for ContentPath {
    type Err = IpfsError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s)
    }
}

impl TryFrom<crate::data::store::scalar::Bytes> for ContentPath {
    type Error = IpfsError;

    fn try_from(bytes: crate::data::store::scalar::Bytes) -> Result<Self, Self::Error> {
        let s = String::from_utf8(bytes.to_vec()).map_err(|err| IpfsError::InvalidContentPath {
            input: bytes.to_string(),
            source: err.into(),
        })?;

        Self::new(s)
    }
}

impl std::fmt::Display for ContentPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let cid = &self.cid;

        match self.path {
            Some(ref path) => write!(f, "{cid}/{path}"),
            None => write!(f, "{cid}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CID_V0: &str = "QmUNLLsPACCz1vLxQVkXqqLX5R1X345qqfHbsf67hvA3Nn";
    const CID_V1: &str = "bafybeiczsscdsbs7ffqz55asqdf3smv6klcw3gofszvwlyarci47bgf354";

    fn make_path(cid: &str, path: Option<&str>) -> ContentPath {
        ContentPath {
            cid: cid.parse().unwrap(),
            path: path.map(ToOwned::to_owned),
        }
    }

    #[test]
    fn fails_on_empty_input() {
        let err = ContentPath::new("").unwrap_err();

        assert_eq!(
            err.to_string(),
            "'' is not a valid IPFS content path: content path is empty",
        );
    }

    #[test]
    fn fails_on_an_invalid_cid() {
        let err = ContentPath::new("not_a_cid").unwrap_err();

        assert!(err
            .to_string()
            .starts_with("'not_a_cid' is not a valid IPFS content path: invalid CID: "));
    }

    #[test]
    fn accepts_a_valid_cid_v0() {
        let path = ContentPath::new(CID_V0).unwrap();
        assert_eq!(path, make_path(CID_V0, None));
    }

    #[test]
    fn accepts_a_valid_cid_v1() {
        let path = ContentPath::new(CID_V1).unwrap();
        assert_eq!(path, make_path(CID_V1, None));
    }

    #[test]
    fn accepts_and_removes_leading_slashes() {
        let path = ContentPath::new(format!("/{CID_V0}")).unwrap();
        assert_eq!(path, make_path(CID_V0, None));

        let path = ContentPath::new(format!("///////{CID_V0}")).unwrap();
        assert_eq!(path, make_path(CID_V0, None));
    }

    #[test]
    fn accepts_and_removes_trailing_slashes() {
        let path = ContentPath::new(format!("{CID_V0}/")).unwrap();
        assert_eq!(path, make_path(CID_V0, None));

        let path = ContentPath::new(format!("{CID_V0}///////")).unwrap();
        assert_eq!(path, make_path(CID_V0, None));
    }

    #[test]
    fn accepts_a_path_after_the_cid() {
        let path = ContentPath::new(format!("{CID_V0}/readme.md")).unwrap();
        assert_eq!(path, make_path(CID_V0, Some("readme.md")));
    }

    #[test]
    fn fails_on_an_invalid_cid_followed_by_a_path() {
        let err = ContentPath::new("not_a_cid/readme.md").unwrap_err();

        assert!(err
            .to_string()
            .starts_with("'not_a_cid/readme.md' is not a valid IPFS content path: invalid CID: "));
    }

    #[test]
    fn fails_on_attempts_to_pass_query_parameters() {
        let err = ContentPath::new(format!("{CID_V0}/readme.md?offline=true")).unwrap_err();

        assert_eq!(
            err.to_string(),
            format!(
                "'{CID_V0}/readme.md?offline=true' is not a valid IPFS content path: query parameters not allowed"
            )
        );
    }

    #[test]
    fn accepts_and_removes_the_ipfs_prefix() {
        let path = ContentPath::new(format!("/ipfs/{CID_V0}")).unwrap();
        assert_eq!(path, make_path(CID_V0, None));

        let path = ContentPath::new(format!("/ipfs/{CID_V0}/readme.md")).unwrap();
        assert_eq!(path, make_path(CID_V0, Some("readme.md")));
    }

    #[test]
    fn accepts_and_removes_the_ipfs_schema() {
        let path = ContentPath::new(format!("ipfs://{CID_V0}")).unwrap();
        assert_eq!(path, make_path(CID_V0, None));

        let path = ContentPath::new(format!("ipfs://{CID_V0}/readme.md")).unwrap();
        assert_eq!(path, make_path(CID_V0, Some("readme.md")));
    }

    #[test]
    fn accepts_and_parses_ipfs_rpc_urls() {
        let path = ContentPath::new(format!("http://ipfs.com/api/v0/cat?arg={CID_V0}")).unwrap();
        assert_eq!(path, make_path(CID_V0, None));

        let path =
            ContentPath::new(format!("http://ipfs.com/api/v0/cat?arg={CID_V0}/readme.md")).unwrap();
        assert_eq!(path, make_path(CID_V0, Some("readme.md")));

        let path = ContentPath::new(format!("https://ipfs.com/api/v0/cat?arg={CID_V0}")).unwrap();
        assert_eq!(path, make_path(CID_V0, None));

        let path = ContentPath::new(format!(
            "https://ipfs.com/api/v0/cat?arg={CID_V0}/readme.md"
        ))
        .unwrap();
        assert_eq!(path, make_path(CID_V0, Some("readme.md")));
    }

    #[test]
    fn accepts_and_parses_ipfs_gateway_urls() {
        let path = ContentPath::new(format!("http://ipfs.com/ipfs/{CID_V0}")).unwrap();
        assert_eq!(path, make_path(CID_V0, None));

        let path = ContentPath::new(format!("http://ipfs.com/ipfs/{CID_V0}/readme.md")).unwrap();
        assert_eq!(path, make_path(CID_V0, Some("readme.md")));

        let path = ContentPath::new(format!("https://ipfs.com/ipfs/{CID_V0}")).unwrap();
        assert_eq!(path, make_path(CID_V0, None));

        let path = ContentPath::new(format!("https://ipfs.com/ipfs/{CID_V0}/readme.md")).unwrap();
        assert_eq!(path, make_path(CID_V0, Some("readme.md")));
    }

    #[test]
    fn accepts_and_parses_paths_from_urls() {
        let path = ContentPath::new(format!("http://ipfs.com/{CID_V0}")).unwrap();
        assert_eq!(path, make_path(CID_V0, None));

        let path = ContentPath::new(format!("http://ipfs.com/{CID_V0}/readme.md")).unwrap();
        assert_eq!(path, make_path(CID_V0, Some("readme.md")));

        let path = ContentPath::new(format!("https://ipfs.com/{CID_V0}")).unwrap();
        assert_eq!(path, make_path(CID_V0, None));

        let path = ContentPath::new(format!("https://ipfs.com/{CID_V0}/readme.md")).unwrap();
        assert_eq!(path, make_path(CID_V0, Some("readme.md")));
    }
}
