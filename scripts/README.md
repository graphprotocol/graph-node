# scripts

## update-cargo-version.sh

This script helps to update the versions of all `Cargo.toml` files and the `Cargo.lock` one.

### Functionality

It does more than what's listed below, but this represents the main behavior of it.

1. Asserts that currently all `Cargo.toml` files in the repository have the **same version**;
2. Changes all of the crates to use the new provided version: `patch`, `minor` or `major`;
3. Updates the `Cargo.lock` via `cargo check --tests`;
4. Adds the changes in a `Release X.Y.Z` commit.

### Usage

The only argument it accepts is the type of version you want to do `(patch|minor|major)`.

```bash
./scripts/update-cargo-version.sh patch
```

Example output:

```
Current version: "0.25.1"
New version: "0.25.2"
Changing 18 toml files
Toml files are still consistent in their version after the update
Updating Cargo.lock file
    Finished dev [unoptimized + debuginfo] target(s) in 0.58s
Cargo.lock file updated
Updating version of the Cargo.{lock, toml} files succeded!
[otavio/update-news-0-25-2 2f2175bae] Release 0.25.2
 20 files changed, 38 insertions(+), 38 deletions(-)
```

This script contains several assertions to make sure no mistake has been made. Unfortunately for now we don't have a way to revert it, or to recover from an error when it fails in the middle of it, this can be improved in the future.
