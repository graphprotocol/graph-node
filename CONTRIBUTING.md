
# Contributing to graph-node

Welcome to the Graph Protocol! Thanks a ton for your interest in contributing.

If you run into any problems feel free to create an issue. PRs are much appreciated for simple things. Here's [a list of good first issues](https://github.com/graphprotocol/graph-node/labels/good%20first%20issue). If it's something more complex we'd appreciate having a quick chat in GitHub Issues or Discord.

Join the conversation on our [Discord](https://discord.gg/graphprotocol).

Please follow the [Code of Conduct](https://github.com/graphprotocol/graph-node/blob/master/CODE_OF_CONDUCT.md) for all the communications and at events. Thank you!

## Development flow

Install development helpers:

```sh
cargo install cargo-watch
rustup component add rustfmt
```

Set environment variables:

```sh
# Only required when testing the Diesel/Postgres store
export THEGRAPH_STORE_POSTGRES_DIESEL_URL=<Postgres database URL>
```

- **Note** You can follow Docker Compose instructions in [store/test-store/README.md](./store/test-store/README.md#docker-compose) to easily run a Postgres instance and use `postgresql://graph:graph@127.0.0.1:5432/graph-test` as the Postgres database URL value.

While developing, a useful command to run in the background is this:

```sh
cargo watch                       \
    -x "fmt --all"                 \
    -x check                      \
    -x "test -- --test-threads=1" \
    -x "doc --no-deps"
```

This will watch your source directory and continuously do the following on changes:

1.  Build all packages in the workspace `target/`.
2.  Generate docs for all packages in the workspace in `target/doc/`.
3.  Automatically format all your source files.

### Integrations Tests

The tests can (and should) be run against a sharded store. See [store/test-store/README.md](./store/test-store/README.md) for
detailed instructions about how to run the sharded integrations tests.

## Commit messages and pull requests

Pull requests are squash-merged: the pull request's title becomes the
commit on master, and its description the commit body. release-please
derives versions and the changelog from those commits, so the title must
follow [Conventional Commits](https://www.conventionalcommits.org) with
the crate as scope: `{type}({crate-name}): {Brief description of changes}`,
for example: `feat(store): Support 'Or' filters`. The types are `feat`,
`fix`, `docs`, `test`, `refactor`, `perf`, `build`, `ci`, `chore` and
`revert`; mark breaking changes with `!` (`feat(store)!: ...`). The
Semantic PR check verifies the title.

If multiple crates are being changed list them all like this:
`feat(core,graphql): Add event source to store`. If all (or most) crates
are affected by the commit, use the scope `all`.

Only `feat`, `fix`, `perf` and breaking changes appear in the changelog;
while the version is below 1.0, `feat` and breaking changes bump the
minor version, `fix` and `perf` the patch version.

The description can be terse, with just enough information to explain
what the change does overall. In a lot of cases, more extensive
explanations of _how_ it achieves its goal are better as comments in the
code.

Commits in a pull request should still be structured in such a way that
each commit consists of a small logical step towards the overall goal of
the pull request, so that the reviewer can follow each change you are
making. For example, it is a good idea to separate simple mechanical
changes like renaming a method that touches many files from logic changes.

Please do not merge master into your branch as you develop your pull
request; instead, rebase your branch on top of the latest master if your
pull request branch is long-lived.

Once your pull request is approved, merge it with the `Squash and merge`
button, checking that the title is the commit message you want on master.
Changes reach master only through pull requests.

## Releases

release-please keeps a release pull request open, `chore(master): release
X.Y.Z`, that bumps the version in `Cargo.toml` and `Cargo.lock` and adds
the changes since the last release to `CHANGELOG.md`. To release:

1. Add the release's notes to `NEWS.md` in a pull request of its own and
   merge it. (Don't edit the release pull request: release-please
   rewrites it on every push to master.)
2. Merge the release pull request. release-please tags `vX.Y.Z` and
   creates the GitHub release, which publishes the graph-node binaries
   and Docker images (`release.yml`) and the gnd binaries and npm
   packages (`gnd-binary-build.yml`).

To release a version other than the one release-please proposes, put
`Release-As: X.Y.Z` in the body of a commit on master (for example in the
squash-merge description of a pull request).
