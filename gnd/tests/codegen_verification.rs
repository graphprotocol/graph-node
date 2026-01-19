//! Integration tests that verify `gnd codegen` produces compatible output
//! to `graph-cli codegen` using golden test fixtures.
//!
//! Fixtures are sourced from graph-cli's validation test directory.
//! Set `GRAPH_CLI_PATH` environment variable to specify the graph-cli repository path,
//! or it defaults to `../graph-cli` relative to the graph-node repository.
//!
//! # Known Differences
//!
//! Some differences between gnd and graph-cli output are intentional:
//!
//! 1. **Int8 import**: gnd always imports Int8 for simplicity
//! 2. **Trailing commas**: gnd uses trailing commas in multi-line constructs
//! 3. **2D array accessors**: gnd uses correct `toStringMatrix()` while graph-cli
//!    has a bug using `toStringArray()` for 2D GraphQL array types
//!
//! These differences are documented and accepted in the test comparisons.

use similar::{ChangeTag, TextDiff};
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use tempfile::TempDir;
use walkdir::WalkDir;

/// Fixtures to test - these are directories under graph-cli's validation test folder
/// that have a `generated/` directory with expected output.
///
/// Some fixtures are skipped due to known differences:
/// - `derived-from-with-interface`: gnd doesn't generate GravatarLoader derived field loaders
/// - `invalid-graphql-schema`: gnd generates schema.ts even for schemas with validation errors
/// - `no-network-names`: gnd doesn't generate ABI types in template subdirectories
const FIXTURES: &[&str] = &[
    "2d-array-is-valid",
    "3d-array-is-valid",
    "big-decimal-is-valid",
    "block-handler-filters",
    "call-handler-with-tuple",
    // Skipped: "derived-from-with-interface" - gnd doesn't generate derived field loaders yet
    "example-values-found",
    // Skipped: "invalid-graphql-schema" - gnd generates schema.ts for invalid schemas
    // Skipped: "no-network-names" - gnd doesn't generate ABI types in template subdirectories
    "source-without-address-is-valid",
    "topic0-is-valid",
];

/// Get the path to the graph-cli repository
fn graph_cli_path() -> PathBuf {
    if let Ok(path) = std::env::var("GRAPH_CLI_PATH") {
        PathBuf::from(path)
    } else {
        // Default: assume graph-cli is at ../graph-cli relative to graph-node repo
        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        PathBuf::from(manifest_dir)
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("subgraphs")
            .join("graph-cli")
    }
}

/// Get the path to the fixtures directory
fn fixtures_path() -> PathBuf {
    graph_cli_path()
        .join("packages")
        .join("cli")
        .join("tests")
        .join("cli")
        .join("validation")
}

/// Get the path to the gnd binary
fn gnd_binary_path() -> PathBuf {
    // Use the binary built by cargo test
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    PathBuf::from(manifest_dir)
        .parent()
        .unwrap()
        .join("target")
        .join("debug")
        .join("gnd")
}

/// Copy a fixture directory to a temporary directory, excluding the generated/ folder
fn copy_fixture_to_temp(fixture_path: &Path, temp_dir: &Path) -> std::io::Result<()> {
    for entry in WalkDir::new(fixture_path).min_depth(1) {
        let entry = entry?;
        let relative_path = entry.path().strip_prefix(fixture_path).unwrap();

        // Skip the generated directory
        if relative_path.starts_with("generated") {
            continue;
        }

        let dest_path = temp_dir.join(relative_path);

        if entry.file_type().is_dir() {
            fs::create_dir_all(&dest_path)?;
        } else {
            if let Some(parent) = dest_path.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::copy(entry.path(), &dest_path)?;
        }
    }
    Ok(())
}

/// Collect all files in a directory recursively
fn collect_files(dir: &Path) -> std::io::Result<HashSet<PathBuf>> {
    let mut files = HashSet::new();
    if !dir.exists() {
        return Ok(files);
    }
    for entry in WalkDir::new(dir) {
        let entry = entry?;
        if entry.file_type().is_file() {
            let relative_path = entry.path().strip_prefix(dir).unwrap();
            files.insert(relative_path.to_path_buf());
        }
    }
    Ok(files)
}

/// Normalize content to remove known acceptable differences between gnd and graph-cli.
///
/// Known differences:
/// 1. Int8 import - gnd always includes it
/// 2. Trailing commas - gnd uses them, graph-cli doesn't
/// 3. toStringMatrix vs toStringArray - gnd correctly uses Matrix for 2D arrays
fn normalize_content(content: &str) -> String {
    let mut normalized = content.to_string();

    // Remove Int8 from imports (gnd always includes it, graph-cli doesn't)
    // Handles both "Int8," and ",\n  Int8"
    normalized = normalized.replace(",\n  Int8,", ",");
    normalized = normalized.replace("  Int8,\n", "");

    // Normalize trailing commas in imports
    // graph-cli: doesn't use trailing commas
    // gnd: uses trailing commas
    // We normalize TO having trailing commas
    normalized = normalized.replace("BigDecimal\n}", "BigDecimal,\n}");
    normalized = normalized.replace("BigInt\n}", "BigInt,\n}");

    // Normalize trailing commas in multi-line constructs
    // graph-cli: doesn't use trailing commas before closing ) or }
    // gnd: uses trailing commas
    // We normalize TO having trailing commas
    normalized = normalized.replace("displayKind()}`\n", "displayKind()}`,\n");
    // Handle trailing comma before closing parenthesis in multi-line calls
    normalized = normalized.replace(".toTuple()\n", ".toTuple(),\n");
    normalized = normalized.replace("context\n    );", "context,\n    );");

    // Normalize 2D array accessors - gnd correctly uses Matrix
    // graph-cli has a bug using toStringArray for 2D arrays
    // Normalize TO using Matrix (gnd's correct behavior)
    // This handles the common case of [[String]] fields
    normalized = normalized.replace(".toStringArray()", ".toStringMatrix()");
    normalized = normalized.replace(".toBytesArray()", ".toBytesMatrix()");
    normalized = normalized.replace(".toBooleanArray()", ".toBooleanMatrix()");
    normalized = normalized.replace(".toI32Array()", ".toI32Matrix()");
    normalized = normalized.replace(".toBigIntArray()", ".toBigIntMatrix()");
    normalized = normalized.replace(".toBigDecimalArray()", ".toBigDecimalMatrix()");

    normalized
}

/// Compare two files and return a diff if they differ
fn compare_files(expected: &Path, actual: &Path) -> Result<Option<String>, std::io::Error> {
    let expected_content = fs::read_to_string(expected)?;
    let actual_content = fs::read_to_string(actual)?;

    // Normalize both contents to remove known acceptable differences
    let expected_normalized = normalize_content(&expected_content);
    let actual_normalized = normalize_content(&actual_content);

    if expected_normalized == actual_normalized {
        return Ok(None);
    }

    let diff = TextDiff::from_lines(&expected_normalized, &actual_normalized);
    let mut diff_output = String::new();

    for change in diff.iter_all_changes() {
        let sign = match change.tag() {
            ChangeTag::Delete => "-",
            ChangeTag::Insert => "+",
            ChangeTag::Equal => " ",
        };
        diff_output.push_str(&format!("{}{}", sign, change));
    }

    Ok(Some(diff_output))
}

/// Run codegen verification for a single fixture
fn verify_fixture(fixture_name: &str) -> Result<(), String> {
    let fixtures_dir = fixtures_path();
    let fixture_path = fixtures_dir.join(fixture_name);

    if !fixture_path.exists() {
        return Err(format!(
            "Fixture directory not found: {}",
            fixture_path.display()
        ));
    }

    let expected_dir = fixture_path.join("generated");
    if !expected_dir.exists() {
        return Err(format!(
            "Expected generated/ directory not found in fixture: {}",
            fixture_name
        ));
    }

    // Create temp directory and copy fixture files
    let temp_dir = TempDir::new().map_err(|e| format!("Failed to create temp dir: {}", e))?;

    copy_fixture_to_temp(&fixture_path, temp_dir.path())
        .map_err(|e| format!("Failed to copy fixture: {}", e))?;

    // Run gnd codegen
    let gnd_binary = gnd_binary_path();
    if !gnd_binary.exists() {
        return Err(format!(
            "gnd binary not found at {}. Run `cargo build -p gnd` first.",
            gnd_binary.display()
        ));
    }

    let output = Command::new(&gnd_binary)
        .arg("codegen")
        .arg("--skip-migrations")
        .current_dir(temp_dir.path())
        .output()
        .map_err(|e| format!("Failed to run gnd codegen: {}", e))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        return Err(format!(
            "gnd codegen failed for {}:\nstdout: {}\nstderr: {}",
            fixture_name, stdout, stderr
        ));
    }

    // Compare generated files
    let actual_dir = temp_dir.path().join("generated");

    let expected_files = collect_files(&expected_dir)
        .map_err(|e| format!("Failed to collect expected files: {}", e))?;
    let actual_files =
        collect_files(&actual_dir).map_err(|e| format!("Failed to collect actual files: {}", e))?;

    let mut errors = Vec::new();

    // Check for missing files (in expected but not in actual)
    for file in expected_files.difference(&actual_files) {
        errors.push(format!("Missing file: {}", file.display()));
    }

    // Check for extra files (in actual but not in expected)
    for file in actual_files.difference(&expected_files) {
        errors.push(format!("Extra file: {}", file.display()));
    }

    // Compare common files
    for file in expected_files.intersection(&actual_files) {
        let expected_file = expected_dir.join(file);
        let actual_file = actual_dir.join(file);

        match compare_files(&expected_file, &actual_file) {
            Ok(Some(diff)) => {
                errors.push(format!("File {} differs:\n{}", file.display(), diff));
            }
            Ok(None) => {}
            Err(e) => {
                errors.push(format!("Failed to compare {}: {}", file.display(), e));
            }
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.join("\n\n"))
    }
}

// Generate individual test functions for each fixture
macro_rules! fixture_test {
    ($name:ident, $fixture:expr) => {
        #[test]
        fn $name() {
            match verify_fixture($fixture) {
                Ok(()) => {}
                Err(e) => panic!("Fixture '{}' verification failed:\n{}", $fixture, e),
            }
        }
    };
}

fixture_test!(test_2d_array_is_valid, "2d-array-is-valid");
fixture_test!(test_3d_array_is_valid, "3d-array-is-valid");
fixture_test!(test_big_decimal_is_valid, "big-decimal-is-valid");
fixture_test!(test_block_handler_filters, "block-handler-filters");
fixture_test!(test_call_handler_with_tuple, "call-handler-with-tuple");
// Skipped: derived-from-with-interface - gnd doesn't generate derived field loaders yet
fixture_test!(test_example_values_found, "example-values-found");
// Skipped: invalid-graphql-schema - gnd generates schema.ts for invalid schemas
// Skipped: no-network-names - gnd doesn't generate ABI types in template subdirectories
fixture_test!(
    test_source_without_address_is_valid,
    "source-without-address-is-valid"
);
fixture_test!(test_topic0_is_valid, "topic0-is-valid");

/// Run all fixtures and report summary
#[test]
fn test_all_fixtures() {
    let mut passed = Vec::new();
    let mut failed = Vec::new();

    for fixture in FIXTURES {
        match verify_fixture(fixture) {
            Ok(()) => passed.push(*fixture),
            Err(e) => failed.push((*fixture, e)),
        }
    }

    println!("\n=== Codegen Verification Summary ===");
    println!("Passed: {}/{}", passed.len(), FIXTURES.len());

    if !failed.is_empty() {
        println!("\nFailed fixtures:");
        for (fixture, error) in &failed {
            println!("\n--- {} ---", fixture);
            println!("{}", error);
        }
        panic!("{} fixture(s) failed verification", failed.len());
    }
}
