use std::path::Path;

use anyhow::Result;
use clap::Parser;

#[derive(Clone, Debug, Parser)]
#[clap(about = "Remove build artifacts and generated files")]
pub struct CleanOpt {
    #[clap(
        long,
        default_value = "generated/",
        help = "Directory where the codegen output is stored"
    )]
    pub codegen_dir: String,

    #[clap(
        long,
        default_value = "build/",
        help = "Directory where the build output is stored"
    )]
    pub build_dir: String,
}

/// Run the clean command
pub fn run_clean(opt: CleanOpt) -> Result<()> {
    println!("Cleaning cache and generated files...");

    let codegen_path = Path::new(&opt.codegen_dir);
    let build_path = Path::new(&opt.build_dir);

    let mut cleaned = false;

    if codegen_path.exists() {
        std::fs::remove_dir_all(codegen_path)?;
        println!("✔ Removed {}", opt.codegen_dir);
        cleaned = true;
    }

    if build_path.exists() {
        std::fs::remove_dir_all(build_path)?;
        println!("✔ Removed {}", opt.build_dir);
        cleaned = true;
    }

    if cleaned {
        println!("Cache and generated files cleaned");
    } else {
        println!("Nothing to clean");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_clean_removes_directories() {
        let temp_dir = TempDir::new().unwrap();
        let codegen_dir = temp_dir.path().join("generated");
        let build_dir = temp_dir.path().join("build");

        // Create the directories
        fs::create_dir(&codegen_dir).unwrap();
        fs::create_dir(&build_dir).unwrap();

        // Create some files in them
        fs::write(codegen_dir.join("schema.ts"), "test").unwrap();
        fs::write(build_dir.join("subgraph.wasm"), "test").unwrap();

        assert!(codegen_dir.exists());
        assert!(build_dir.exists());

        let opt = CleanOpt {
            codegen_dir: codegen_dir.to_string_lossy().to_string(),
            build_dir: build_dir.to_string_lossy().to_string(),
        };

        run_clean(opt).unwrap();

        assert!(!codegen_dir.exists());
        assert!(!build_dir.exists());
    }

    #[test]
    fn test_clean_handles_missing_directories() {
        let temp_dir = TempDir::new().unwrap();
        let codegen_dir = temp_dir.path().join("nonexistent_generated");
        let build_dir = temp_dir.path().join("nonexistent_build");

        let opt = CleanOpt {
            codegen_dir: codegen_dir.to_string_lossy().to_string(),
            build_dir: build_dir.to_string_lossy().to_string(),
        };

        // Should not error when directories don't exist
        run_clean(opt).unwrap();
    }
}
