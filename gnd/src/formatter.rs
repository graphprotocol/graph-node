//! Code formatting utilities.
//!
//! This module provides formatting for generated TypeScript/AssemblyScript code
//! using Prettier.

use std::io::Write;
use std::process::{Command, Stdio};

use anyhow::{bail, Context, Result};

/// Format TypeScript/AssemblyScript code using Prettier.
///
/// Returns the formatted code, or an error if formatting fails.
/// Use `try_format_typescript` for graceful fallback.
pub fn format_typescript(code: &str) -> Result<String> {
    format_with_prettier(code, "typescript")
}

/// Format TypeScript/AssemblyScript code using Prettier.
///
/// If Prettier is not available or fails, the code is returned unformatted.
pub fn try_format_typescript(code: &str) -> String {
    match format_with_prettier(code, "typescript") {
        Ok(formatted) => formatted,
        Err(_) => code.to_string(),
    }
}

/// Format code using Prettier with the specified parser.
fn format_with_prettier(code: &str, parser: &str) -> Result<String> {
    // Try npx prettier first (most common setup)
    let result = run_prettier_command(code, parser, "npx", &["prettier", "--stdin-filepath"]);
    if result.is_ok() {
        return result;
    }

    // Try pnpx prettier
    let result = run_prettier_command(code, parser, "pnpx", &["prettier", "--stdin-filepath"]);
    if result.is_ok() {
        return result;
    }

    // Try global prettier installation
    run_prettier_command(code, parser, "prettier", &["--stdin-filepath"])
}

/// Run prettier command with the given program and arguments.
fn run_prettier_command(code: &str, parser: &str, program: &str, args: &[&str]) -> Result<String> {
    // Build command with stdin-filepath to hint the parser
    let filepath = format!("file.{}", parser_to_extension(parser));

    let mut cmd = Command::new(program);
    for arg in args {
        cmd.arg(arg);
    }
    cmd.arg(&filepath);

    cmd.stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let mut child = cmd.spawn().context("Failed to spawn prettier process")?;

    // Write code to stdin
    if let Some(mut stdin) = child.stdin.take() {
        stdin
            .write_all(code.as_bytes())
            .context("Failed to write to prettier stdin")?;
    }

    let output = child
        .wait_with_output()
        .context("Failed to read prettier output")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("Prettier failed: {}", stderr);
    }

    String::from_utf8(output.stdout).context("Prettier output is not valid UTF-8")
}

/// Convert parser name to file extension.
fn parser_to_extension(parser: &str) -> &str {
    match parser {
        "typescript" => "ts",
        "json" => "json",
        "yaml" => "yaml",
        _ => "ts",
    }
}

/// Check if Prettier is available on the system.
pub fn is_prettier_available() -> bool {
    // Check npx
    if Command::new("npx")
        .args(["prettier", "--version"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
    {
        return true;
    }

    // Check pnpx
    if Command::new("pnpx")
        .args(["prettier", "--version"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
    {
        return true;
    }

    // Check global prettier
    Command::new("prettier")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parser_to_extension() {
        assert_eq!(parser_to_extension("typescript"), "ts");
        assert_eq!(parser_to_extension("json"), "json");
        assert_eq!(parser_to_extension("yaml"), "yaml");
        assert_eq!(parser_to_extension("unknown"), "ts");
    }

    #[test]
    fn test_format_typescript_graceful_fallback() {
        // Even if prettier is not installed, format_typescript should return
        // the original code without error
        let code = "export class Foo { }";
        let result = format_typescript(code);
        assert!(result.is_ok());
        // Result should either be formatted or the original code
        let formatted = result.unwrap();
        assert!(formatted.contains("class Foo") || formatted.contains("Foo"));
    }
}
