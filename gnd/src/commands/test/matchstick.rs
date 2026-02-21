//! Backward-compatible Matchstick test runner (legacy mode).
//!
//! Dispatches to Docker mode or binary mode depending on the `--docker` flag.
//! This is the legacy path for projects that haven't migrated to the new
//! JSON-based test format yet.

use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::output::{step, Step};

use super::TestOpt;

const MATCHSTICK_GITHUB_RELEASES: &str =
    "https://api.github.com/repos/LimeChain/matchstick/releases/latest";
const MATCHSTICK_DOWNLOAD_BASE: &str = "https://github.com/LimeChain/matchstick/releases/download";
const MATCHSTICK_FALLBACK_VERSION: &str = "0.6.0";
const VERSION_CACHE_TTL_SECS: u64 = 86400; // 24 hours

/// Cached version info written to `{test_dir}/.latest.json`.
#[derive(Serialize, Deserialize)]
struct VersionCache {
    version: String,
    timestamp: u64,
}

pub(super) async fn run(opt: &TestOpt) -> Result<()> {
    if opt.docker {
        run_docker_tests(opt).await
    } else {
        run_binary_tests(opt).await
    }
}

/// Resolve the Matchstick version to use.
///
/// Priority: CLI flag → cached `.latest.json` (24h TTL) → GitHub API → fallback.
async fn resolve_matchstick_version(
    explicit_version: Option<&str>,
    cache_dir: &Path,
) -> Result<String> {
    if let Some(v) = explicit_version {
        return Ok(v.to_string());
    }

    let cache_path = cache_dir.join(".latest.json");

    if let Some(cached) = read_version_cache(&cache_path) {
        return Ok(cached);
    }

    step(Step::Load, "Fetching latest Matchstick version");
    match fetch_latest_version().await {
        Ok(version) => {
            let _ = write_version_cache(&cache_path, &version);
            Ok(version)
        }
        Err(e) => {
            step(
                Step::Warn,
                &format!(
                    "Failed to fetch latest version ({}), using {}",
                    e, MATCHSTICK_FALLBACK_VERSION
                ),
            );
            Ok(MATCHSTICK_FALLBACK_VERSION.to_string())
        }
    }
}

async fn fetch_latest_version() -> Result<String> {
    let client = reqwest::Client::builder().user_agent("gnd-cli").build()?;

    let resp: serde_json::Value = client
        .get(MATCHSTICK_GITHUB_RELEASES)
        .send()
        .await
        .context("Failed to reach GitHub API")?
        .error_for_status()
        .context("GitHub API returned an error")?
        .json()
        .await
        .context("Failed to parse GitHub API response")?;

    resp["tag_name"]
        .as_str()
        .map(|s| s.to_string())
        .ok_or_else(|| anyhow!("GitHub API response missing tag_name"))
}

fn read_version_cache(path: &Path) -> Option<String> {
    let data = std::fs::read_to_string(path).ok()?;
    let cache: VersionCache = serde_json::from_str(&data).ok()?;
    let now = SystemTime::now().duration_since(UNIX_EPOCH).ok()?.as_secs();

    if now.saturating_sub(cache.timestamp) < VERSION_CACHE_TTL_SECS {
        Some(cache.version)
    } else {
        None
    }
}

fn write_version_cache(path: &Path, version: &str) -> Result<()> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("System clock before UNIX epoch")?
        .as_secs();

    let cache = VersionCache {
        version: version.to_string(),
        timestamp: now,
    };

    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, serde_json::to_string_pretty(&cache)?)?;
    Ok(())
}

/// Platform-specific binary name for a Matchstick version.
/// Mirrors getPlatform from graph-tooling: versions > 0.5.4 use simplified names.
fn get_platform(version: &str) -> Result<String> {
    let ver = semver::Version::parse(version)?;
    let cutoff = semver::Version::new(0, 5, 4);

    let os = std::env::consts::OS;
    let arch = std::env::consts::ARCH;

    if arch != "x86_64" && !(os == "macos" && arch == "aarch64") {
        return Err(anyhow!("Unsupported platform: {} {}", os, arch));
    }

    if ver > cutoff {
        match os {
            "macos" if arch == "aarch64" => Ok("binary-macos-12-m1".to_string()),
            "macos" => Ok("binary-macos-12".to_string()),
            "linux" => Ok("binary-linux-22".to_string()),
            _ => Err(anyhow!("Unsupported OS: {}", os)),
        }
    } else {
        // Legacy platform detection for versions <= 0.5.4
        match os {
            "macos" => {
                let darwin_major = get_darwin_major_version();
                if matches!(darwin_major, Some(18) | Some(19)) {
                    Ok("binary-macos-10.15".to_string())
                } else if arch == "aarch64" {
                    Ok("binary-macos-11-m1".to_string())
                } else {
                    Ok("binary-macos-11".to_string())
                }
            }
            "linux" => {
                let linux_major = get_linux_major_version();
                match linux_major {
                    Some(18) => Ok("binary-linux-18".to_string()),
                    Some(22) | Some(24) => Ok("binary-linux-22".to_string()),
                    _ => Ok("binary-linux-20".to_string()),
                }
            }
            _ => Err(anyhow!("Unsupported OS: {}", os)),
        }
    }
}

/// Darwin kernel major version from `uname -r`. Darwin 18/19 → macOS 10.14/10.15.
fn get_darwin_major_version() -> Option<u32> {
    let output = std::process::Command::new("uname")
        .arg("-r")
        .output()
        .ok()?;
    let release = String::from_utf8_lossy(&output.stdout);
    release.trim().split('.').next()?.parse().ok()
}

fn get_linux_major_version() -> Option<u32> {
    let content = std::fs::read_to_string("/etc/os-release").ok()?;
    for line in content.lines() {
        if let Some(val) = line.strip_prefix("VERSION_ID=") {
            let val = val.trim_matches('"');
            // Handle "22.04" → 22, or "22" → 22
            return val.split('.').next()?.parse().ok();
        }
    }
    None
}

/// Download the Matchstick binary to `node_modules/.bin/matchstick-{platform}`. Skips if already exists.
async fn download_matchstick_binary(version: &str, platform: &str, force: bool) -> Result<PathBuf> {
    let bin_dir = PathBuf::from("node_modules/.bin");
    let bin_path = bin_dir.join(format!("matchstick-{platform}"));

    if bin_path.exists() && !force {
        step(
            Step::Done,
            &format!("Binary already exists: {}", bin_path.display()),
        );
        return Ok(bin_path);
    }

    std::fs::create_dir_all(&bin_dir)?;

    let url = format!("{MATCHSTICK_DOWNLOAD_BASE}/{version}/{platform}");
    step(Step::Load, &format!("Downloading Matchstick {version}"));

    let client = reqwest::Client::builder().user_agent("gnd-cli").build()?;

    let resp = client.get(&url).send().await?.error_for_status()?;

    let bytes = resp
        .bytes()
        .await
        .context("Failed to read download response")?;

    std::fs::write(&bin_path, &bytes)?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&bin_path, std::fs::Permissions::from_mode(0o755))?;
    }

    step(Step::Done, &format!("Downloaded to {}", bin_path.display()));
    Ok(bin_path)
}

async fn run_binary_tests(opt: &TestOpt) -> Result<()> {
    step(Step::Generate, "Running Matchstick tests (legacy mode)");

    let version = resolve_matchstick_version(
        opt.matchstick_version.as_deref(),
        Path::new(super::DEFAULT_TEST_DIR),
    )
    .await?;

    let platform = get_platform(&version)?;
    let bin_path = download_matchstick_binary(&version, &platform, opt.force).await?;

    let workdir = opt
        .manifest
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let mut cmd = std::process::Command::new(&bin_path);
    cmd.current_dir(workdir);

    if opt.coverage {
        cmd.arg("-c");
    }
    if opt.recompile {
        cmd.arg("-r");
    }
    if let Some(datasource) = &opt.datasource {
        cmd.arg(datasource);
    }

    let status = cmd.status()?;

    if status.success() {
        step(Step::Done, "Matchstick tests passed");
        Ok(())
    } else {
        Err(anyhow!("Matchstick tests failed"))
    }
}

/// Run Matchstick tests in Docker (recommended on macOS where the native binary is bugged).
async fn run_docker_tests(opt: &TestOpt) -> Result<()> {
    step(Step::Generate, "Running Matchstick tests in Docker");

    std::process::Command::new("docker")
        .arg("--version")
        .output()
        .context("Docker not found. Please install Docker to use -d/--docker mode.")?;

    let mut test_args = String::new();
    if opt.coverage {
        test_args.push_str(" -c");
    }
    if opt.recompile {
        test_args.push_str(" -r");
    }
    if let Some(datasource) = &opt.datasource {
        // Validate datasource name to prevent shell injection via Docker's
        // `sh -c "matchstick $ARGS"` expansion.
        if !datasource
            .chars()
            .all(|c| c.is_alphanumeric() || c == '_' || c == '-')
        {
            anyhow::bail!(
                "Invalid datasource name '{}': must contain only alphanumeric characters, hyphens, or underscores",
                datasource
            );
        }
        test_args.push_str(&format!(" {}", datasource));
    }

    let cwd = std::env::current_dir().context("Failed to get current directory")?;

    let mut cmd = std::process::Command::new("docker");
    cmd.args([
        "run",
        "-it",
        "--rm",
        "--mount",
        &format!("type=bind,source={},target=/matchstick", cwd.display()),
    ]);
    if !test_args.is_empty() {
        cmd.args(["-e", &format!("ARGS={}", test_args.trim())]);
    }
    cmd.arg("matchstick");

    // Check if the Docker image already exists.
    let image_check = std::process::Command::new("docker")
        .args(["images", "-q", "matchstick"])
        .output()
        .context("Failed to check for Docker image")?;
    let image_exists = !image_check.stdout.is_empty();

    if !image_exists || opt.force {
        let version = resolve_matchstick_version(
            opt.matchstick_version.as_deref(),
            Path::new(super::DEFAULT_TEST_DIR),
        )
        .await?;

        step(Step::Generate, "Building Matchstick Docker image");
        let dockerfile_path = PathBuf::from("tests/.docker/Dockerfile");
        if !dockerfile_path.exists() || opt.force {
            create_dockerfile(&dockerfile_path, &version)?;
        }
        let build_status = std::process::Command::new("docker")
            .args([
                "build",
                "-f",
                &dockerfile_path.to_string_lossy(),
                "-t",
                "matchstick",
                ".",
            ])
            .status()
            .context("Failed to build Docker image")?;
        if !build_status.success() {
            return Err(anyhow!("Failed to build Matchstick Docker image"));
        }
    }

    let status = cmd.status().context("Failed to run Docker container")?;
    if status.success() {
        step(Step::Done, "Tests passed");
        Ok(())
    } else {
        Err(anyhow!("Tests failed"))
    }
}

/// Generate a Dockerfile that downloads the Matchstick runner binary (not from npm).
fn create_dockerfile(path: &PathBuf, version: &str) -> Result<()> {
    use std::fs;

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let dockerfile_content = format!(
        r#"FROM --platform=linux/x86_64 ubuntu:22.04

ARG DEBIAN_FRONTEND=noninteractive
ENV ARGS=""

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
     curl ca-certificates postgresql postgresql-contrib \
  && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL https://deb.nodesource.com/setup_18.x | bash - \
  && apt-get install -y --no-install-recommends nodejs \
  && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL -o /usr/local/bin/matchstick \
     https://github.com/LimeChain/matchstick/releases/download/{version}/binary-linux-22 \
  && chmod +x /usr/local/bin/matchstick

RUN mkdir /matchstick
WORKDIR /matchstick

CMD ["sh", "-c", "matchstick $ARGS"]
"#,
        version = version
    );

    fs::write(path, dockerfile_content)?;
    step(Step::Write, &format!("Created {}", path.display()));
    Ok(())
}
