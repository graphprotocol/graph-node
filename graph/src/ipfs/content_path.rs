use anyhow::anyhow;
use cid::Cid;

use crate::ipfs::IpfsError;
use crate::ipfs::IpfsResult;

#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
/// Represents a path to some data on IPFS.
pub struct ContentPath {
    cid: Cid,
    path: Option<String>,
}

impl ContentPath {
    /// Creates a new [ContentPath] from the specified input.
    pub fn new(input: impl AsRef<str>) -> IpfsResult<Self> {
        let input = input.as_ref();

        if input.is_empty() {
            return Err(IpfsError::InvalidContentPath {
                input: "".to_owned(),
                source: anyhow!("path is empty"),
            });
        }

        let (cid, path) = input
            .strip_prefix("/ipfs/")
            .unwrap_or(input)
            .split_once('/')
            .unwrap_or((input, ""));

        let cid = cid
            .parse::<Cid>()
            .map_err(|err| IpfsError::InvalidContentPath {
                input: input.to_owned(),
                source: anyhow::Error::from(err).context("invalid CID"),
            })?;

        if path.contains('?') {
            return Err(IpfsError::InvalidContentPath {
                input: input.to_owned(),
                source: anyhow!("query parameters not allowed"),
            });
        }

        Ok(Self {
            cid,
            path: (!path.is_empty()).then_some(path.to_owned()),
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

    #[test]
    fn fails_on_empty_input() {
        let err = ContentPath::new("").unwrap_err();

        assert_eq!(
            err.to_string(),
            "'' is not a valid IPFS content path: path is empty",
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

        assert_eq!(
            path,
            ContentPath {
                cid: CID_V0.parse().unwrap(),
                path: None,
            }
        );
    }

    #[test]
    fn accepts_a_valid_cid_v1() {
        let path = ContentPath::new(CID_V1).unwrap();

        assert_eq!(
            path,
            ContentPath {
                cid: CID_V1.parse().unwrap(),
                path: None,
            }
        );
    }

    #[test]
    fn fails_on_a_leading_slash_followed_by_a_valid_cid() {
        let err = ContentPath::new(format!("/{CID_V0}")).unwrap_err();

        assert!(err.to_string().starts_with(&format!(
            "'/{CID_V0}' is not a valid IPFS content path: invalid CID: "
        )));
    }

    #[test]
    fn ignores_the_first_slash_after_the_cid() {
        let path = ContentPath::new(format!("{CID_V0}/")).unwrap();

        assert_eq!(
            path,
            ContentPath {
                cid: CID_V0.parse().unwrap(),
                path: None,
            }
        );
    }

    #[test]
    fn accepts_a_path_after_the_cid() {
        let path = ContentPath::new(format!("{CID_V0}/readme.md")).unwrap();

        assert_eq!(
            path,
            ContentPath {
                cid: CID_V0.parse().unwrap(),
                path: Some("readme.md".to_owned()),
            }
        );
    }

    #[test]
    fn accepts_multiple_consecutive_slashes_after_the_cid() {
        let path = ContentPath::new(format!("{CID_V0}//")).unwrap();

        assert_eq!(
            path,
            ContentPath {
                cid: CID_V0.parse().unwrap(),
                path: Some("/".to_owned()),
            }
        );
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

        assert_eq!(
            path,
            ContentPath {
                cid: CID_V0.parse().unwrap(),
                path: None,
            }
        );

        let path = ContentPath::new(format!("/ipfs/{CID_V0}/readme.md")).unwrap();

        assert_eq!(
            path,
            ContentPath {
                cid: CID_V0.parse().unwrap(),
                path: Some("readme.md".to_owned()),
            }
        );
    }
}
