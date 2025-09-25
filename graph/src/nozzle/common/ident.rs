use std::{
    cmp::Ordering,
    fmt,
    hash::{Hash, Hasher},
    sync::Arc,
};

use anyhow::{bail, Result};
use heck::{ToLowerCamelCase, ToSnakeCase, ToUpperCamelCase};
use lazy_regex::regex_is_match;

use crate::derive::CheapClone;

/// Represents a valid identifier that can be used for SQL table names, SQL column names,
/// entity names and entity fields.
///
/// Validates and tokenizes an identifier to allow case-insensitive and format-insensitive
/// comparison between multiple identifiers.
///
/// Maintains the original identifier for cases when the exact format is required after comparisons.
///
/// # Example
///
/// ```rust
/// # use graph::nozzle::common::Ident;
///
/// assert_eq!(Ident::new("block_hash").unwrap(), Ident::new("blockHash").unwrap());
/// assert_eq!(Ident::new("block-hash").unwrap(), Ident::new("BlockHash").unwrap());
/// ```
#[derive(Debug, Clone, CheapClone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Ident(Arc<Inner>);

impl Ident {
    /// Creates a new identifier.
    ///
    /// Validates and tokenizes an identifier to allow case-insensitive and format-insensitive
    /// comparison between multiple identifiers.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The input string `s` does not start with a letter or an underscore
    /// - The input string `s` does not contain only letters, numbers, hyphens, and underscores
    /// - The input string `s` contains more than 100 characters
    ///
    /// The returned error is deterministic.
    pub fn new(s: impl AsRef<str>) -> Result<Self> {
        let raw = s.as_ref();

        if !regex_is_match!("^[a-zA-Z_][a-zA-Z0-9_-]{0,100}$", raw) {
            bail!("invalid identifier '{raw}': must start with a letter or an underscore, and contain only letters, numbers, hyphens, and underscores");
        }

        Ok(Self(Arc::new(Inner::new(raw))))
    }

    /// Returns a reference to the original string used to create this identifier.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use graph::nozzle::common::Ident;
    ///
    /// let ident = Ident::new("BLOCK_hash").unwrap();
    /// assert_eq!(ident.as_str(), "BLOCK_hash");
    /// ```
    pub fn as_str(&self) -> &str {
        &self.0.raw
    }

    /// Returns the tokens of this identifier that are used for case-insensitive and format-insensitive comparison.
    ///
    /// A token is a sequence of lowercase characters between case format separators.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use graph::nozzle::common::Ident;
    ///
    /// let ident = Ident::new("blockHash").unwrap();
    /// assert_eq!(ident.tokens(), &["block".into(), "hash".into()]);
    /// ```
    pub fn tokens(&self) -> &[Box<str>] {
        &self.0.tokens
    }

    /// Converts this identifier to `lowerCamelCase` format.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use graph::nozzle::common::Ident;
    ///
    /// let ident = Ident::new("block_hash").unwrap();
    /// assert_eq!(ident.to_lower_camel_case(), "blockHash");
    /// ```
    pub fn to_lower_camel_case(&self) -> String {
        self.0.raw.to_lower_camel_case()
    }

    /// Converts this identifier to `UpperCamelCase` format.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use graph::nozzle::common::Ident;
    ///
    /// let ident = Ident::new("block_hash").unwrap();
    /// assert_eq!(ident.to_upper_camel_case(), "BlockHash");
    /// ```
    pub fn to_upper_camel_case(&self) -> String {
        self.0.raw.to_upper_camel_case()
    }
}

impl fmt::Display for Ident {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0.raw)
    }
}

/// The internal representation of an identifier.
#[derive(Debug)]
struct Inner {
    /// The original unmodified string used to create the identifier.
    raw: Box<str>,

    /// The tokens of the identifier that are used for case-insensitive
    /// and format-insensitive comparison.
    tokens: Box<[Box<str>]>,
}

impl Inner {
    /// Creates a new internal representation of an identifier.
    fn new(raw: &str) -> Self {
        let tokens = raw
            .to_snake_case()
            .split('_')
            .map(Into::into)
            .collect::<Vec<_>>()
            .into();

        Self {
            raw: raw.into(),
            tokens,
        }
    }
}

impl PartialEq for Inner {
    fn eq(&self, other: &Self) -> bool {
        self.tokens == other.tokens
    }
}

impl Eq for Inner {}

impl PartialOrd for Inner {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.tokens.cmp(&other.tokens))
    }
}

impl Ord for Inner {
    fn cmp(&self, other: &Self) -> Ordering {
        self.tokens.cmp(&other.tokens)
    }
}

impl Hash for Inner {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.tokens.hash(state);
    }
}
