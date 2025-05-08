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
}

impl FileLinkResolver {
    /// Create a new FileLinkResolver
    ///
    /// All paths are treated as absolute paths.
    pub fn new() -> Self {
        Self {
            base_dir: None,
            timeout: Duration::from_secs(30),
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
        }
    }

    fn resolve_path(&self, link: &str) -> PathBuf {
        let path = Path::new(link);

        // Return the path as is if base_dir is None, or join with base_dir if present.
        // if "link" is an absolute path, join will simply return that path.
        self.base_dir
            .as_ref()
            .map_or_else(|| path.to_owned(), |base_dir| base_dir.join(link))
    }
}

pub fn remove_prefix(link: &str) -> &str {
    const IPFS: &str = "/ipfs/";
    if link.starts_with(IPFS) {
        &link[IPFS.len()..]
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
        let resolver = FileLinkResolver::new();
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
}
