# Spec: gnd CLI Expansion

This spec describes the expansion of `gnd` (Graph Node Dev) to include functionality currently provided by the TypeScript-based `graph-cli`. The goal is a drop-in replacement for subgraph development workflows, implemented in Rust and integrated with graph-node's existing infrastructure.

## Goals

- **Drop-in replacement**: Same commands, flags, output format, and exit codes as `graph-cli`
- **Identical code generation**: AssemblyScript output must be byte-for-byte identical after formatting
- **Ethereum only**: Initial scope limited to Ethereum protocol
- **Reuse graph-node internals**: Leverage existing manifest parsing, validation, IPFS client, etc.

## Non-Goals

- Multi-protocol support (NEAR, Cosmos, Arweave, Substreams) - future work
- Rollout/migration plan from TS CLI
- Performance optimization beyond reasonable behavior

## Commands

### Command Matrix

| Command   | TS CLI | gnd | Notes                                      |
| --------- | ------ | --- | ------------------------------------------ |
| `codegen` | Yes    | Yes | Generate AssemblyScript types              |
| `build`   | Yes    | Yes | Compile to WASM                            |
| `deploy`  | Yes    | Yes | Deploy to Graph Node                       |
| `init`    | Yes    | Yes | Scaffold new subgraph                      |
| `add`     | Yes    | Yes | Add datasource to existing subgraph        |
| `remove`  | Yes    | Yes | Unregister subgraph name                   |
| `create`  | Yes    | Yes | Register subgraph name                     |
| `auth`    | Yes    | Yes | Set deploy key                             |
| `publish` | Yes    | Yes | Publish to decentralized network           |
| `test`    | Yes    | Yes | Run Matchstick tests                       |
| `clean`   | Yes    | Yes | Remove build artifacts                     |
| `dev`     | No     | Yes | Run graph-node in dev mode (existing gnd)  |
| `local`   | Yes    | No  | Skipped - use existing test infrastructure |
| `node`    | Yes    | No  | Skipped - use graphman for node management |

### Known Differences from TS CLI

**Commands:**

1. **`local` command**: Not implemented. Users should use existing integration test infrastructure.
2. **`node` subcommand**: Not implemented. Use `graphman` for node management operations.
3. **`--uncrashable` flag on codegen**: Not implemented. Float Capital's uncrashable helper generation is a niche third-party feature.
4. **Debug output**: Uses `RUST_LOG` environment variable instead of `DEBUG=graph-cli:*`.

**Code generation** (intentional, documented in verification tests): 5. **Int8 import**: gnd always imports `Int8` for simplicity, even when not used. 6. **Trailing commas**: gnd uses trailing commas in multi-line constructs. 7. **2D array accessors**: gnd uses correct `toStringMatrix()` while graph-cli has a bug using `toStringArray()` for 2D GraphQL array types. 8. **Tuple/struct component names**: gnd correctly extracts component names from raw ABI JSON, while graph-cli's ethabi parser loses nested component names for some tuple types. 9. **Subgraph data source file naming**: gnd uses the data source name (`subgraph-SourceName.ts`) instead of IPFS hash (`subgraph-QmHash.ts`) for stable import paths that don't break when source subgraphs are redeployed.

## CLI Interface

### Binary and Invocation

```
gnd <command> [options] [arguments]
```

The binary name is `gnd`. All graph-cli commands become gnd subcommands.

### Version Output

```
$ gnd --version
gnd 0.1.0 (graph-cli compatible: 0.98.1)
```

Shows both gnd version and the graph-cli version it emulates.

### Flag Compatibility

All flags must match the TS CLI exactly:

- Same long names (`--output-dir`)
- Same short names (`-o`)
- Same defaults
- Same validation behavior

Reference: Each command section below lists flags with references to TS CLI source.

### Output Format

Output must match TS CLI format exactly, including:

- Spinner/progress indicators
- Success checkmarks (`✔`)
- Step descriptions
- File paths displayed
- Error formatting (information must match; exact wording may differ)

Reference: `/packages/cli/src/command-helpers/spinner.ts`

### Exit Codes

Exit codes must match TS CLI behavior:

- `0`: Success
- `1`: Error (validation, compilation, deployment failure, etc.)

### Configuration Files

Use same paths and formats as TS CLI:

| File           | Path                           | Purpose                       |
| -------------- | ------------------------------ | ----------------------------- |
| Auth tokens    | `~/.graphprotocol/`            | Deploy keys and access tokens |
| Network config | `networks.json` (project root) | Network-specific addresses    |

Reference: `/packages/cli/src/command-helpers/auth.ts`

## Command Specifications

### `gnd codegen`

Generates AssemblyScript types from subgraph manifest.

**Usage:**

```
gnd codegen [subgraph-manifest]
```

**Arguments:**

- `subgraph-manifest`: Path to manifest file (default: `subgraph.yaml`)

**Flags:**
| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--output-dir` | `-o` | `generated/` | Output directory for generated types |
| `--skip-migrations` | | `false` | Skip subgraph migrations |
| `--watch` | `-w` | `false` | Regenerate on file changes |
| `--ipfs` | `-i` | `https://api.thegraph.com/ipfs/` | IPFS node URL |
| `--help` | `-h` | | Show help |

**Behavior:**

1. Load and validate manifest
2. Apply migrations (unless `--skip-migrations`)
3. Assert minimum API version (0.0.5) and graph-ts version (0.25.0)
4. Generate entity classes from GraphQL schema
5. Generate ABI bindings for each contract
6. Generate template datasource bindings
7. Format output with prettier
8. Write to output directory

**Output Structure:**

```
generated/
├── schema.ts                           # Entity classes
├── <DataSourceName>/
│   └── <ContractName>.ts              # ABI bindings
├── templates/
│   └── <TemplateName>/
│       └── <ContractName>.ts          # Template ABI bindings
└── subgraph-<DataSourceName>.ts       # Entity types for subgraph data sources
```

**TS CLI References:**

- Command: `/packages/cli/src/commands/codegen.ts`
- Type generator: `/packages/cli/src/type-generator.ts`
- Schema codegen: `/packages/cli/src/codegen/schema.ts`
- ABI codegen: `/packages/cli/src/protocols/ethereum/codegen/abi.ts`

### `gnd build`

Compiles subgraph to WASM.

**Usage:**

```
gnd build [subgraph-manifest]
```

**Arguments:**

- `subgraph-manifest`: Path to manifest file (default: `subgraph.yaml`)

**Flags:**
| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--output-dir` | `-o` | `build/` | Output directory |
| `--output-format` | `-t` | `wasm` | Output format: `wasm` or `wast` |
| `--skip-migrations` | | `false` | Skip subgraph migrations |
| `--watch` | `-w` | `false` | Rebuild on file changes |
| `--ipfs` | `-i` | | IPFS node URL (uploads if provided) |
| `--network` | | | Network to use from networks.json |
| `--network-file` | | `networks.json` | Path to networks config |
| `--help` | `-h` | | Show help |

**Behavior:**

1. Run codegen (unless types already exist)
2. Apply migrations (unless `--skip-migrations`)
3. Resolve network-specific values from networks.json
4. Shell out to `asc` (AssemblyScript compiler) for each mapping
5. Copy ABIs and schema to build directory
6. Copy template ABIs to build/templates/<name>/ directories
7. Generate build manifest
8. Optionally upload to IPFS

**Build Output Structure:**

```
build/
├── schema.graphql
├── subgraph.yaml
├── <DataSourceName>/
│   ├── <DataSourceName>.wasm
│   └── <ContractName>.json          # ABI
└── templates/
    └── <TemplateName>/
        ├── <TemplateName>.wasm
        └── <ContractName>.json      # ABI
```

**Note:** When multiple data sources or templates share the same mapping.ts file, gnd compiles it once and copies the resulting WASM to all required output locations.

**TS CLI References:**

- Command: `/packages/cli/src/commands/build.ts`
- Compiler: `/packages/cli/src/compiler/index.ts`

### `gnd deploy`

Deploys subgraph to a Graph Node.

**Usage:**

```
gnd deploy [subgraph-name] [subgraph-manifest]
```

**Arguments:**

- `subgraph-name`: Name to deploy as (e.g., `user/subgraph`)
- `subgraph-manifest`: Path to manifest file (default: `subgraph.yaml`)

**Flags:**
| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--product` | | | Product: `subgraph-studio` or `hosted-service` |
| `--studio` | | `false` | Shorthand for `--product subgraph-studio` |
| `--node` | `-g` | | Graph Node URL |
| `--ipfs` | `-i` | | IPFS node URL |
| `--access-token` | | | Access token for authentication |
| `--deploy-key` | | | Deploy key (alias for access-token) |
| `--version-label` | `-l` | | Version label |
| `--headers` | | | Additional HTTP headers (JSON) |
| `--debug-fork` | | | Fork subgraph for debugging |
| `--skip-migrations` | | `false` | Skip subgraph migrations |
| `--network` | | | Network from networks.json |
| `--network-file` | | `networks.json` | Path to networks config |
| `--output-dir` | `-o` | `build/` | Build output directory |
| `--help` | `-h` | | Show help |

**Deploy Targets:**

- Local Graph Node (via `--node` and `--ipfs`)
- Subgraph Studio (`--product subgraph-studio` or `--studio`)
- Hosted Service (`--product hosted-service`)
- Decentralized network (via `publish` command)

**Behavior:**

1. Build subgraph (runs build command)
2. Upload build artifacts to IPFS
3. Send deployment request to Graph Node via JSON-RPC

**TS CLI References:**

- Command: `/packages/cli/src/commands/deploy.ts`

### `gnd init`

Scaffolds a new subgraph project.

**Usage:**

```
gnd init [directory]
```

**Arguments:**

- `directory`: Directory to create subgraph in

**Flags:**
| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--protocol` | | | Protocol: `ethereum` |
| `--product` | | | Product for deployment |
| `--studio` | | `false` | Initialize for Subgraph Studio |
| `--from-contract` | | | Contract address to generate from |
| `--from-example` | | | Example subgraph to clone |
| `--contract-name` | | | Name for the contract |
| `--index-events` | | `false` | Index all contract events |
| `--start-block` | | | Start block for indexing |
| `--network` | | `mainnet` | Network name |
| `--abi` | | | Path to ABI file |
| `--spkg` | | | Path to Substreams package |
| `--allow-simple-name` | | `false` | Allow simple subgraph names |
| `--help` | `-h` | | Show help |

**Example Subgraphs:**

The `--from-example <name>` flag clones from `https://github.com/graphprotocol/graph-tooling/tree/main/examples`.
The example name is **required** - no default is provided. Available examples include:
`ethereum-gravatar`, `aggregations`, `ethereum-basic-event-handlers`, `substreams-powered-subgraph`, etc.
Legacy name `ethereum/gravatar` is automatically converted to `ethereum-gravatar`.

**Behavior:**

1. Prompt for missing information (protocol, network, contract, etc.)
2. Fetch ABI from Etherscan/Sourcify if `--from-contract` and no `--abi`
3. Generate scaffold:
   - `subgraph.yaml` manifest
   - `schema.graphql` with entities for events
   - `src/mapping.ts` with event handlers
   - `package.json` with dependencies
   - `tsconfig.json`
   - ABIs directory
4. Optionally initialize git repository
5. Install dependencies

**External APIs:**

- Etherscan API: Fetch verified contract ABIs
- Sourcify API: Fetch verified contract ABIs (fallback)
- Network registry: `@pinax/graph-networks-registry` for chain configuration

**TS CLI References:**

- Command: `/packages/cli/src/commands/init.ts`
- Scaffold: `/packages/cli/src/scaffold/index.ts`
- Schema generation: `/packages/cli/src/scaffold/schema.ts`
- Mapping generation: `/packages/cli/src/scaffold/mapping.ts`
- Etherscan client: `/packages/cli/src/command-helpers/contracts.ts`

### `gnd add`

Adds a new datasource to an existing subgraph.

**Usage:**

```
gnd add <address> [subgraph-manifest]
```

**Arguments:**

- `address`: Contract address
- `subgraph-manifest`: Path to manifest file (default: `subgraph.yaml`)

**Flags:**
| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--abi` | | | Path to ABI file |
| `--contract-name` | | | Name for the contract |
| `--merge-entities` | | `false` | Merge with existing entities |
| `--network-file` | | `networks.json` | Path to networks config |
| `--start-block` | | | Start block |
| `--help` | `-h` | | Show help |

**TS CLI References:**

- Command: `/packages/cli/src/commands/add.ts`

### `gnd create`

Registers a subgraph name with a Graph Node.

**Usage:**

```
gnd create <subgraph-name>
```

**Arguments:**

- `subgraph-name`: Name to register

**Flags:**
| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--node` | `-g` | | Graph Node URL |
| `--access-token` | | | Access token |
| `--help` | `-h` | | Show help |

**TS CLI References:**

- Command: `/packages/cli/src/commands/create.ts`

### `gnd remove`

Unregisters a subgraph name from a Graph Node.

**Usage:**

```
gnd remove <subgraph-name>
```

**Arguments:**

- `subgraph-name`: Name to unregister

**Flags:**
| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--node` | `-g` | | Graph Node URL |
| `--access-token` | | | Access token |
| `--help` | `-h` | | Show help |

**TS CLI References:**

- Command: `/packages/cli/src/commands/remove.ts`

### `gnd auth`

Sets the deploy key for a Graph Node.

**Usage:**

```
gnd auth <deploy-key>
```

**Arguments:**

- `deploy-key`: Deploy key to store

**Flags:**
| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--product` | | | Product: `subgraph-studio` or `hosted-service` |
| `--studio` | | `false` | Shorthand for subgraph-studio |
| `--help` | `-h` | | Show help |

**Behavior:**
Stores deploy key in `~/.graphprotocol/` for later use by deploy/publish commands.

**TS CLI References:**

- Command: `/packages/cli/src/commands/auth.ts`
- Auth helpers: `/packages/cli/src/command-helpers/auth.ts`

### `gnd publish`

Publishes a subgraph to The Graph's decentralized network.

**Usage:**

```
gnd publish [subgraph-manifest]
```

**Arguments:**

- `subgraph-manifest`: Path to manifest file (default: `subgraph.yaml`)

**Flags:**
| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--subgraph-id` | | | Subgraph ID to publish to |
| `--ipfs` | `-i` | | IPFS node URL |
| `--protocol-network` | | | Protocol network (e.g., `arbitrum-one`) |
| `--help` | `-h` | | Show help |

**TS CLI References:**

- Command: `/packages/cli/src/commands/publish.ts`

### `gnd test`

Runs Matchstick tests for the subgraph.

**Usage:**

```
gnd test [datasource]
```

**Arguments:**

- `datasource`: Specific datasource to test (optional)

**Flags:**
| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--coverage` | `-c` | `false` | Run with coverage |
| `--docker` | `-d` | `false` | Run in Docker container |
| `--force` | `-f` | `false` | Force recompilation |
| `--logs` | `-l` | `false` | Show logs |
| `--recompile` | `-r` | `false` | Recompile before testing |
| `--version` | `-v` | | Matchstick version |
| `--help` | `-h` | | Show help |

**Behavior:**

1. Download Matchstick binary (if not present)
2. Shell out to Matchstick with appropriate flags
3. Report test results

**TS CLI References:**

- Command: `/packages/cli/src/commands/test.ts`

### `gnd clean`

Removes build artifacts and generated files.

**Usage:**

```
gnd clean
```

**Flags:**
| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--codegen-dir` | | `generated/` | Codegen output directory |
| `--build-dir` | | `build/` | Build output directory |
| `--help` | `-h` | | Show help |

**Behavior:**
Removes `generated/` and `build/` directories (or custom paths if specified).

**TS CLI References:**

- Command: `/packages/cli/src/commands/clean.ts`

### `gnd dev`

Runs graph-node in development mode with file watching.

This is the existing `gnd` functionality, preserved as a subcommand. The implementation can be adjusted to fit the new subcommand structure.

**Usage:**

```
gnd dev [options]
```

**Flags:**
Preserve existing gnd flags, adjusted as needed for subcommand structure.

## Code Generation

Code generation is the most complex component and must produce byte-for-byte identical output to the TS CLI (after prettier formatting).

### Generated File Types

#### 1. Entity Classes (`schema.ts`)

Generated from GraphQL schema. Each entity type becomes an AssemblyScript class with:

- Constructor
- Static `load(id)` method
- Static `loadInBlock(id)` method
- `save()` method
- Getters/setters for each field
- Proper type mappings (GraphQL → AssemblyScript)

**TS CLI Reference:** `/packages/cli/src/codegen/schema.ts`

#### 2. ABI Bindings (`<Contract>.ts`)

Generated from contract ABI. Includes:

- Event classes with typed parameters
- Function call result classes
- Contract class with typed call methods
- Proper Ethereum type mappings

**Tuple/Struct Handling:**
When an event or function has tuple parameters with named components (e.g., a struct), gnd generates proper struct classes with named getters:

```typescript
class AssetTransfer__ParamsAssetStruct extends ethereum.Tuple {
  get addr(): Address {
    return this[0].toAddress();
  }
  get amount(): BigInt {
    return this[1].toBigInt();
  }
  get active(): boolean {
    return this[2].toBoolean();
  }
}
```

gnd parses the raw ABI JSON to extract component names, which ethabi loses during parsing.

**TS CLI Reference:** `/packages/cli/src/protocols/ethereum/codegen/abi.ts`

#### 3. Template Bindings

Generated for template datasources with the same structure as ABI bindings.

**TS CLI Reference:** `/packages/cli/src/codegen/template.ts`

#### 4. Subgraph Data Source Bindings (`subgraph-<DataSourceName>.ts`)

Generated for `kind: subgraph` data sources. When a manifest contains a subgraph data source:

```yaml
dataSources:
  - kind: subgraph
    name: SourceSubgraph
    source:
      address: "QmRWTEejPDDwALaquFGm6X2GBbbh5osYDXwCRRkoZ6KQhb"
```

gnd will:

1. Validate that all subgraph data source names are unique (error if duplicates found)
2. Fetch the referenced subgraph's manifest from IPFS
3. Extract and fetch the schema from the manifest
4. Generate entity types (without store methods) to `generated/subgraph-{DataSourceName}.ts`

**Note:** gnd uses the data source name (e.g., `SourceSubgraph`) for the generated filename rather than the IPFS hash. This produces stable import paths like `import { Entity } from '../generated/subgraph-SourceSubgraph'` that don't break when the source subgraph is redeployed with a new hash. This differs from graph-cli which uses the IPFS hash.

The generated types include entity classes with getters for all fields, but no `save()`, `load()`, or `loadInBlock()` methods since these are read-only types from the source subgraph.

**TS CLI Reference:** `/packages/cli/src/type-generator.ts` lines 72-136

### Type Mappings

#### GraphQL → AssemblyScript

| GraphQL          | AssemblyScript |
| ---------------- | -------------- |
| `ID`             | `string`       |
| `String`         | `string`       |
| `Int`            | `i32`          |
| `BigInt`         | `BigInt`       |
| `BigDecimal`     | `BigDecimal`   |
| `Bytes`          | `Bytes`        |
| `Boolean`        | `boolean`      |
| `[T]`            | `Array<T>`     |
| Entity reference | `string` (ID)  |

**TS CLI Reference:** `/packages/cli/src/codegen/schema.ts` (look for type mapping functions)

#### Ethereum ABI → AssemblyScript

| Solidity  | AssemblyScript  |
| --------- | --------------- |
| `address` | `Address`       |
| `bool`    | `boolean`       |
| `bytes`   | `Bytes`         |
| `bytesN`  | `Bytes`         |
| `intN`    | `BigInt`        |
| `uintN`   | `BigInt`        |
| `string`  | `string`        |
| `T[]`     | `Array<T>`      |
| tuple     | Generated class |

**TS CLI Reference:** `/packages/cli/src/protocols/ethereum/codegen/abi.ts`

### API Version Handling

Different `apiVersion` values in the manifest affect code generation. gnd must support all versions that the TS CLI supports.

**TS CLI Reference:** `/packages/cli/src/codegen/` (version-specific logic throughout)

### Formatting

All generated code must be formatted with prettier before writing:

- Shell out to `prettier` with same configuration as TS CLI
- Parser: `typescript`

## Migrations

Migrations update older manifest formats to newer versions. gnd must implement all migrations that TS CLI supports.

**Migration Chain:**

```
0.0.1 → 0.0.2 → 0.0.3 → 0.0.4 → 0.0.5 → ... → current
```

**TS CLI Reference:** `/packages/cli/src/migrations/`

Each migration is a transformation function that:

1. Checks manifest version
2. Applies necessary changes
3. Updates version number

## External Dependencies

### Runtime Dependencies (shell out)

| Tool         | Purpose                 | Required      |
| ------------ | ----------------------- | ------------- |
| `asc`        | AssemblyScript compiler | For `build`   |
| `prettier`   | Code formatting         | For `codegen` |
| `matchstick` | Test runner             | For `test`    |

### Network APIs

| API                              | Purpose                                 |
| -------------------------------- | --------------------------------------- |
| Etherscan                        | Fetch verified contract ABIs            |
| Sourcify                         | Fetch verified contract ABIs (fallback) |
| `@pinax/graph-networks-registry` | Network configuration (chain IDs, etc.) |

The network registry should be fetched at runtime to get current network configurations.

### graph-node Reuse

| Component           | graph-node Location                   | Purpose                     |
| ------------------- | ------------------------------------- | --------------------------- |
| Manifest parsing    | `graph/src/data/subgraph/`            | Load subgraph.yaml          |
| Manifest validation | `graph/src/data/subgraph/`            | Validate manifest structure |
| GraphQL schema      | `graph/src/schema/input/`             | Parse schema.graphql        |
| IPFS client         | `graph/src/ipfs/`                     | Upload to IPFS              |
| Link resolver       | `graph/src/components/link_resolver/` | Resolve file references     |
| File watcher        | `gnd/src/watcher.rs`                  | Watch mode                  |

Refactor graph-node components as needed to make them reusable.

## Module Structure

```
gnd/src/
├── main.rs                    # Entry point, clap setup
├── lib.rs
├── formatter.rs               # Prettier integration for code formatting
├── prompt.rs                  # Interactive prompts (network selection, etc.)
├── watcher.rs                 # File watcher for --watch modes
├── commands/
│   ├── mod.rs
│   ├── add.rs
│   ├── auth.rs
│   ├── build.rs
│   ├── clean.rs
│   ├── codegen.rs
│   ├── create.rs
│   ├── deploy.rs
│   ├── dev.rs                 # Existing gnd functionality
│   ├── init.rs
│   ├── publish.rs
│   ├── remove.rs
│   └── test.rs
├── codegen/
│   ├── mod.rs
│   ├── abi.rs                 # ABI binding generation
│   ├── schema.rs              # Entity class generation
│   ├── template.rs            # Template binding generation
│   ├── types.rs               # Type conversion utilities
│   └── typescript.rs          # AST builders for TypeScript/AssemblyScript
├── scaffold/
│   ├── mod.rs
│   ├── manifest.rs            # Generate subgraph.yaml
│   ├── mapping.rs             # Generate mapping.ts
│   └── schema.rs              # Generate schema.graphql
├── migrations/
│   ├── mod.rs
│   ├── api_version.rs         # API version migrations
│   ├── spec_version.rs        # Spec version migrations
│   └── versions.rs            # Version definitions
├── compiler/
│   ├── mod.rs
│   └── asc.rs                 # Shell out to asc
├── services/
│   ├── mod.rs
│   ├── contract.rs            # ABI fetching (Etherscan, Blockscout, Sourcify, registry)
│   ├── graph_node.rs          # Graph Node JSON-RPC client
│   └── ipfs.rs                # IPFS client for uploads
├── config/
│   ├── mod.rs
│   └── networks.rs            # networks.json handling
└── output/
    ├── mod.rs
    └── spinner.rs             # Progress/spinner output
```

## Testing

### Current Status

- **166 unit tests** passing (`cargo test -p gnd --lib`)
- **12 codegen verification tests** passing (`cargo test -p gnd --test codegen_verification`)
- **7 CLI command tests** passing (`cargo test -p gnd --test cli_commands`)
- Test fixtures from graph-cli validation tests in `gnd/tests/fixtures/codegen_verification/`

### Test Strategy

1. **Unit tests**: Test individual functions (type mappings, migrations, etc.)
2. **Snapshot tests**: Compare generated output against TS CLI output
3. **Integration tests**: End-to-end command execution

### Test Corpus

Use the same test corpus as graph-cli. Tests must cover at least everything the TS CLI tests cover.

**TS CLI Test Location:** `/packages/cli/tests/`

### Codegen Verification Tests

Located in `gnd/tests/codegen_verification.rs`, these tests verify compatibility with graph-cli:

1. Copy fixture to temp directory (excluding `generated/`)
2. Run `gnd codegen` on the fixture
3. Compare output against expected `generated/` directory
4. Allow known differences (Int8 import, trailing commas, 2D array fix)

Fixtures can be regenerated with `./tests/fixtures/regenerate.sh`.

### Edge Cases

When edge case bugs are discovered in the TS CLI, gnd should fix them rather than replicate them. Document any behavioral differences that result from bug fixes.

### CLI Integration Tests

Located in `tests/tests/gnd_cli_tests.rs`, these tests verify gnd works as a drop-in replacement for graph-cli by running the integration test suite with `GRAPH_CLI` environment variable pointing to the gnd binary.

**How it works:**

- The integration test infrastructure uses `CONFIG.graph_cli` for deployment commands
- Setting `GRAPH_CLI=../target/debug/gnd` makes tests use gnd instead of graph-cli
- `Subgraph::deploy()` calls `gnd codegen`, `gnd create`, `gnd deploy`

**Commands tested:**

- `gnd codegen` - Generate AssemblyScript types
- `gnd create` - Register subgraph name with Graph Node
- `gnd deploy` - Deploy subgraph to Graph Node

**Running:**

```bash
just test-gnd-cli
```

### Standalone Command Tests

Located in `gnd/tests/cli_commands.rs`, these tests verify commands that don't require a running Graph Node:

**Commands tested:**

- `gnd init` - Scaffold generation (--from-example, --from-contract, --from-subgraph)
- `gnd add` - Add datasource to existing subgraph
- `gnd build` - WASM compilation

**Running:**

```bash
just test-gnd-commands
```

## Dependencies to Add

### Cargo.toml additions

```toml
[dependencies]
# CLI framework (already present)
clap = { version = "...", features = ["derive"] }

# Interactive prompts (for init)
inquire = "..."

# HTTP client (for Etherscan, Sourcify, registry)
reqwest = { version = "...", features = ["json"] }

# Progress/spinner output
indicatif = "..."

# Template rendering (for scaffold)
minijinja = "..."

# JSON handling
serde_json = "..."
```

## Open Questions

None at this time. All major decisions have been made.

## References

- TS CLI repository: https://github.com/graphprotocol/graph-tooling
- TS CLI source: `/packages/cli/src/`
- Local checkout: `/home/lutter/code/subgraphs/graph-cli`
- Original gnd expansion plan: `/LOCAL/plans/gnd-cli-expansion.md`
