# Scripts

## `release.sh`

1. Checks that all workspace crates use the same version via `version.workspace = true`.
2. Updates the version in the root `Cargo.toml` as indicated by the user: `major`, `minor`, or `patch`.
3. Updates `Cargo.lock` via `cargo check --tests`.
4. Adds the changes in a `Release vX.Y.Z` commit.

Upon failure, the script will print some kind of error message and stop before committing the changes.

### Usage

The only argument it accepts is the type of release you want to do.

```bash
# E.g. you're on v0.28.0 and must relese v0.28.1.
$ ./scripts/release.sh patch
```
