use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::anyhow;
use async_trait::async_trait;
use slog::Logger;

use crate::data::subgraph::Link;
use crate::prelude::{Error, JsonValueStream, LinkResolver as LinkResolverTrait};

#[derive(Clone, Debug)]
pub struct FileLinkResolver {
    base_dir: Option<PathBuf>,
    timeout: Duration,
    // This is a hashmap that maps the alias name to the path of the file that is aliased
    aliases: HashMap<String, PathBuf>,
}

impl Default for FileLinkResolver {
    fn default() -> Self {
        Self {
            base_dir: None,
            timeout: Duration::from_secs(30),
            aliases: HashMap::new(),
        }
    }
}

impl FileLinkResolver {
    /// Create a new FileLinkResolver
    ///
    /// All paths are treated as absolute paths.
    pub fn new(base_dir: Option<PathBuf>, aliases: HashMap<String, PathBuf>) -> Self {
        Self {
            base_dir: base_dir,
            timeout: Duration::from_secs(30),
            aliases,
        }
    }

    /// Create a new FileLinkResolver with a base directory
    ///
    /// All paths that are not absolute will be considered
    /// relative to this base directory.
    pub fn with_base_dir<P: AsRef<Path>>(base_dir: P) -> Self {
        Self {
            base_dir: Some(base_dir.as_ref().to_owned()),
            timeout: Duration::from_secs(30),
            aliases: HashMap::new(),
        }
    }

    fn resolve_path(&self, link: &str) -> PathBuf {
        let path = Path::new(link);

        // If the path is an alias, use the aliased path
        if let Some(aliased) = self.aliases.get(link) {
            return aliased.clone();
        }

        // If the path is already absolute or if we don't have a base_dir, return it as is
        if path.is_absolute() || self.base_dir.is_none() {
            path.to_owned()
        } else {
            // Otherwise, join with base_dir
            self.base_dir.as_ref().unwrap().join(link)
        }
    }

    /// This method creates a new resolver that is scoped to a specific subgraph
    /// It will set the base directory to the parent directory of the manifest path
    /// This is required because paths mentioned in the subgraph manifest are relative paths
    /// and we need a new resolver with the right base directory for the specific subgraph
    fn clone_for_manifest(&self, manifest_path_str: &str) -> Result<Self, Error> {
        let mut resolver = self.clone();

        // Create a path to the manifest based on the current resolver's
        // base directory or default to using the deployment string as path
        // If the deployment string is an alias, use the aliased path
        let manifest_path = if let Some(aliased) = self.aliases.get(&manifest_path_str.to_string())
        {
            aliased.clone()
        } else {
            match &resolver.base_dir {
                Some(dir) => dir.join(&manifest_path_str),
                None => PathBuf::from(manifest_path_str),
            }
        };

        let canonical_manifest_path = manifest_path
            .canonicalize()
            .map_err(|e| Error::from(anyhow!("Failed to canonicalize manifest path: {}", e)))?;

        // The manifest path is the path of the subgraph manifest file in the build directory
        // We use the parent directory as the base directory for the new resolver
        let base_dir = canonical_manifest_path
            .parent()
            .ok_or_else(|| Error::from(anyhow!("Manifest path has no parent directory")))?
            .to_path_buf();

        resolver.base_dir = Some(base_dir);
        Ok(resolver)
    }
}

pub fn remove_prefix(link: &str) -> &str {
    if link.starts_with("/ipfs/") {
        &link[6..] // Skip the "/ipfs/" prefix (6 characters)
    } else {
        link
    }
}

#[async_trait]
impl LinkResolverTrait for FileLinkResolver {
    fn with_timeout(&self, timeout: Duration) -> Box<dyn LinkResolverTrait> {
        let mut resolver = self.clone();
        resolver.timeout = timeout;
        Box::new(resolver)
    }

    fn with_retries(&self) -> Box<dyn LinkResolverTrait> {
        Box::new(self.clone())
    }

    async fn cat(&self, logger: &Logger, link: &Link) -> Result<Vec<u8>, Error> {
        let link = remove_prefix(&link.link);
        let path = self.resolve_path(&link);

        slog::debug!(logger, "File resolver: reading file"; 
            "path" => path.to_string_lossy().to_string());

        match tokio::fs::read(&path).await {
            Ok(data) => Ok(data),
            Err(e) => {
                slog::error!(logger, "Failed to read file"; 
                    "path" => path.to_string_lossy().to_string(),
                    "error" => e.to_string());
                Err(anyhow!("Failed to read file {}: {}", path.display(), e).into())
            }
        }
    }

    fn for_manifest(&self, manifest_path: &str) -> Result<Box<dyn LinkResolverTrait>, Error> {
        Ok(Box::new(self.clone_for_manifest(manifest_path)?))
    }

    async fn get_block(&self, _logger: &Logger, _link: &Link) -> Result<Vec<u8>, Error> {
        Err(anyhow!("get_block is not implemented for FileLinkResolver").into())
    }

    async fn json_stream(&self, _logger: &Logger, _link: &Link) -> Result<JsonValueStream, Error> {
        Err(anyhow!("json_stream is not implemented for FileLinkResolver").into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use std::fs;
    use std::io::Write;

    #[tokio::test]
    async fn test_file_resolver_absolute() {
        // Test the resolver without a base directory (absolute paths only)

        // Create a temporary directory for test files
        let temp_dir = env::temp_dir().join("file_resolver_test");
        let _ = fs::create_dir_all(&temp_dir);

        // Create a test file in the temp directory
        let test_file_path = temp_dir.join("test.txt");
        let test_content = b"Hello, world!";
        let mut file = fs::File::create(&test_file_path).unwrap();
        file.write_all(test_content).unwrap();

        // Create a resolver without a base directory
        let resolver = FileLinkResolver::default();
        let logger = slog::Logger::root(slog::Discard, slog::o!());

        // Test valid path resolution
        let link = Link {
            link: test_file_path.to_string_lossy().to_string(),
        };
        let resolved_path = resolver.resolve_path(&link.link);
        println!("Absolute mode - Resolved path: {:?}", resolved_path);
        let result = resolver.cat(&logger, &link).await.unwrap();
        assert_eq!(result, test_content);

        // Test path with leading slash that likely doesn't exist
        let link = Link {
            link: "/test.txt".to_string(),
        };
        let resolved_path = resolver.resolve_path(&link.link);
        println!(
            "Absolute mode - Path with leading slash: {:?}",
            resolved_path
        );
        let result = resolver.cat(&logger, &link).await;
        assert!(
            result.is_err(),
            "Reading /test.txt should fail as it doesn't exist"
        );

        // Clean up
        let _ = fs::remove_file(test_file_path);
        let _ = fs::remove_dir(temp_dir);
    }

    #[tokio::test]
    async fn test_file_resolver_with_base_dir() {
        // Test the resolver with a base directory

        // Create a temporary directory for test files
        let temp_dir = env::temp_dir().join("file_resolver_test_base_dir");
        let _ = fs::create_dir_all(&temp_dir);

        // Create a test file in the temp directory
        let test_file_path = temp_dir.join("test.txt");
        let test_content = b"Hello from base dir!";
        let mut file = fs::File::create(&test_file_path).unwrap();
        file.write_all(test_content).unwrap();

        // Create a resolver with a base directory
        let resolver = FileLinkResolver::with_base_dir(&temp_dir);
        let logger = slog::Logger::root(slog::Discard, slog::o!());

        println!("Base directory mode - base dir: {:?}", temp_dir);

        // Test relative path (no leading slash)
        let link = Link {
            link: "test.txt".to_string(),
        };
        let resolved_path = resolver.resolve_path(&link.link);
        println!(
            "Base directory mode - Resolved relative path: {:?}",
            resolved_path
        );
        let result = resolver.cat(&logger, &link).await.unwrap();
        assert_eq!(result, test_content);

        // Test relative path with leading slash (should be treated as absolute on Unix)
        let link = Link {
            link: "/test.txt".to_string(),
        };
        let resolved_path = resolver.resolve_path(&link.link);
        println!(
            "Base directory mode - Resolved path with leading slash: {:?}",
            resolved_path
        );

        println!(
            "Result for path with leading slash: {:?}",
            resolver.cat(&logger, &link).await
        );

        // Test absolute path
        let link = Link {
            link: test_file_path.to_string_lossy().to_string(),
        };
        let resolved_path = resolver.resolve_path(&link.link);
        println!(
            "Base directory mode - Resolved absolute path: {:?}",
            resolved_path
        );
        let result = resolver.cat(&logger, &link).await.unwrap();
        assert_eq!(result, test_content);

        // Test missing file
        let link = Link {
            link: "missing.txt".to_string(),
        };
        let result = resolver.cat(&logger, &link).await;
        assert!(result.is_err());

        // Clean up
        let _ = fs::remove_file(test_file_path);
        let _ = fs::remove_dir(temp_dir);
    }

    #[tokio::test]
    async fn test_file_resolver_with_aliases() {
        // Create a temporary directory for test files
        let temp_dir = env::temp_dir().join("file_resolver_test_aliases");
        let _ = fs::create_dir_all(&temp_dir);

        // Create two test files with different content
        let test_file1_path = temp_dir.join("file.txt");
        let test_content1 = b"This is the file content";
        let mut file1 = fs::File::create(&test_file1_path).unwrap();
        file1.write_all(test_content1).unwrap();

        let test_file2_path = temp_dir.join("another_file.txt");
        let test_content2 = b"This is another file content";
        let mut file2 = fs::File::create(&test_file2_path).unwrap();
        file2.write_all(test_content2).unwrap();

        // Create aliases mapping
        let mut aliases = HashMap::new();
        aliases.insert("alias1".to_string(), test_file1_path.clone());
        aliases.insert("alias2".to_string(), test_file2_path.clone());
        aliases.insert("deployment-id".to_string(), test_file1_path.clone());

        // Create resolver with aliases
        let resolver = FileLinkResolver::new(Some(temp_dir.clone()), aliases);
        let logger = slog::Logger::root(slog::Discard, slog::o!());

        // Test resolving by aliases
        let link1 = Link {
            link: "alias1".to_string(),
        };
        let result1 = resolver.cat(&logger, &link1).await.unwrap();
        assert_eq!(result1, test_content1);

        let link2 = Link {
            link: "alias2".to_string(),
        };
        let result2 = resolver.cat(&logger, &link2).await.unwrap();
        assert_eq!(result2, test_content2);

        // Test that the alias works in for_deployment as well
        let deployment_resolver = resolver.clone_for_manifest("deployment-id").unwrap();

        let expected_dir = test_file1_path.parent().unwrap();
        let deployment_base_dir = deployment_resolver.base_dir.clone().unwrap();

        let canonical_expected_dir = expected_dir.canonicalize().unwrap();
        let canonical_deployment_dir = deployment_base_dir.canonicalize().unwrap();

        assert_eq!(
            canonical_deployment_dir, canonical_expected_dir,
            "Build directory paths don't match"
        );

        // Clean up
        let _ = fs::remove_file(test_file1_path);
        let _ = fs::remove_file(test_file2_path);
        let _ = fs::remove_dir(temp_dir);
    }
}
