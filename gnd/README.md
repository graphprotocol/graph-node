# gnd - Graph Node Dev CLI

A drop-in replacement for `graph-cli` written in Rust. `gnd` provides all the subgraph development commands with identical output, flags, and behavior.

## Installation

Build from source:

```bash
cargo build -p gnd --release
```

The binary will be at `target/release/gnd`.

## Quick Start

```bash
# Create a new subgraph from a contract
gnd init --from-contract 0x1234... --network mainnet my-subgraph

# Generate AssemblyScript types
gnd codegen

# Build the subgraph
gnd build

# Deploy to a local Graph Node
gnd create --node http://localhost:8020 my-name/my-subgraph
gnd deploy --node http://localhost:8020 --ipfs http://localhost:5001 my-name/my-subgraph

# Or deploy to Subgraph Studio
gnd auth YOUR_DEPLOY_KEY
gnd deploy my-name/my-subgraph
```

## Commands

### `gnd init`

Create a new subgraph with basic scaffolding.

```bash
gnd init [SUBGRAPH_NAME] [DIRECTORY]
```

**Flags:**

| Flag | Short | Description |
|------|-------|-------------|
| `--protocol` | | Protocol: `ethereum`, `near`, `cosmos`, `arweave`, `substreams` |
| `--from-contract` | | Create from an existing contract address |
| `--from-example` | | Create from an example subgraph template |
| `--from-subgraph` | | Create from an existing deployed subgraph |
| `--contract-name` | | Name for the contract (with `--from-contract`) |
| `--index-events` | | Index all contract events as entities |
| `--network` | | Network the contract is deployed to |
| `--start-block` | | Block number to start indexing from |
| `--abi` | | Path to the contract ABI file |
| `--node` | `-g` | Graph Node URL |
| `--ipfs` | `-i` | IPFS node URL |
| `--skip-install` | | Skip installing npm dependencies |
| `--skip-git` | | Skip initializing a Git repository |

**Examples:**

```bash
# Interactive mode (prompts for all options)
gnd init

# From contract with ABI fetched from Etherscan
gnd init --from-contract 0x1234... --network mainnet my-subgraph

# From contract with local ABI
gnd init --from-contract 0x1234... --abi ./MyContract.json my-subgraph

# From an existing deployed subgraph
gnd init --from-subgraph QmHash... my-subgraph
```

### `gnd codegen`

Generate AssemblyScript types from the subgraph manifest.

```bash
gnd codegen [MANIFEST]
```

**Arguments:**
- `MANIFEST`: Path to subgraph manifest (default: `subgraph.yaml`)

**Flags:**

| Flag | Short | Description |
|------|-------|-------------|
| `--output-dir` | `-o` | Output directory (default: `generated/`) |
| `--skip-migrations` | | Skip manifest migrations |
| `--watch` | `-w` | Regenerate on file changes |
| `--ipfs` | `-i` | IPFS node URL |

**Examples:**

```bash
# Generate types
gnd codegen

# Generate to custom directory
gnd codegen -o src/generated/

# Watch mode
gnd codegen --watch
```

### `gnd build`

Compile the subgraph to WASM.

```bash
gnd build [MANIFEST]
```

**Arguments:**
- `MANIFEST`: Path to subgraph manifest (default: `subgraph.yaml`)

**Flags:**

| Flag | Short | Description |
|------|-------|-------------|
| `--output-dir` | `-o` | Output directory (default: `build/`) |
| `--output-format` | `-t` | Output format: `wasm` or `wast` (default: `wasm`) |
| `--skip-migrations` | | Skip manifest migrations |
| `--watch` | `-w` | Rebuild on file changes |
| `--ipfs` | `-i` | IPFS node URL (uploads if provided) |
| `--network` | | Network from networks.json |
| `--network-file` | | Path to networks config (default: `networks.json`) |

**Examples:**

```bash
# Build subgraph
gnd build

# Build and upload to IPFS
gnd build --ipfs http://localhost:5001

# Build for specific network
gnd build --network mainnet
```

### `gnd deploy`

Deploy a subgraph to a Graph Node.

```bash
gnd deploy <SUBGRAPH_NAME> [MANIFEST]
```

**Arguments:**
- `SUBGRAPH_NAME`: Name to deploy as (e.g., `user/subgraph`)
- `MANIFEST`: Path to subgraph manifest (default: `subgraph.yaml`)

**Flags:**

| Flag | Short | Description |
|------|-------|-------------|
| `--node` | `-g` | Graph Node URL (defaults to Subgraph Studio) |
| `--ipfs` | `-i` | IPFS node URL |
| `--deploy-key` | | Deploy key for authentication |
| `--version-label` | `-l` | Version label for the deployment |
| `--ipfs-hash` | | IPFS hash of already-uploaded manifest |
| `--output-dir` | `-o` | Build output directory (default: `build/`) |
| `--skip-migrations` | | Skip manifest migrations |
| `--network` | | Network from networks.json |
| `--network-file` | | Path to networks config |
| `--debug-fork` | | Fork subgraph ID for debugging |

**Examples:**

```bash
# Deploy to Subgraph Studio (uses saved auth key)
gnd deploy my-name/my-subgraph

# Deploy to local Graph Node
gnd deploy --node http://localhost:8020 --ipfs http://localhost:5001 my-name/my-subgraph

# Deploy with version label
gnd deploy -l v1.0.0 my-name/my-subgraph
```

### `gnd publish`

Publish a subgraph to The Graph's decentralized network.

```bash
gnd publish [MANIFEST]
```

**Arguments:**
- `MANIFEST`: Path to subgraph manifest (default: `subgraph.yaml`)

**Flags:**

| Flag | Short | Description |
|------|-------|-------------|
| `--ipfs` | `-i` | IPFS node URL |
| `--ipfs-hash` | | Skip build, use existing IPFS hash |
| `--subgraph-id` | | Subgraph ID for updating existing subgraphs |
| `--protocol-network` | | Network: `arbitrum-one` or `arbitrum-sepolia` |
| `--api-key` | | API key (required when updating existing) |
| `--output-dir` | `-o` | Build output directory |
| `--skip-migrations` | | Skip manifest migrations |
| `--network` | | Network from networks.json |
| `--network-file` | | Path to networks config |

**Examples:**

```bash
# Publish new subgraph
gnd publish

# Update existing subgraph
gnd publish --subgraph-id Qm... --api-key YOUR_KEY
```

### `gnd add`

Add a new data source to an existing subgraph.

```bash
gnd add <ADDRESS> [MANIFEST]
```

**Arguments:**
- `ADDRESS`: Contract address to add
- `MANIFEST`: Path to subgraph manifest (default: `subgraph.yaml`)

**Flags:**

| Flag | Short | Description |
|------|-------|-------------|
| `--abi` | | Path to the contract ABI |
| `--contract-name` | | Name for the new data source |
| `--merge-entities` | | Merge with existing entities of same name |
| `--network` | | Network the contract is deployed to |
| `--start-block` | | Block number to start indexing from |

**Examples:**

```bash
# Add contract with ABI from Etherscan
gnd add 0x1234... --network mainnet

# Add contract with local ABI
gnd add 0x1234... --abi ./NewContract.json --contract-name MyContract
```

### `gnd create`

Register a subgraph name with a Graph Node.

```bash
gnd create <SUBGRAPH_NAME> --node <URL>
```

**Flags:**

| Flag | Short | Description |
|------|-------|-------------|
| `--node` | `-g` | Graph Node URL (required) |
| `--access-token` | | Access token for authentication |

### `gnd remove`

Unregister a subgraph name from a Graph Node.

```bash
gnd remove <SUBGRAPH_NAME> --node <URL>
```

**Flags:**

| Flag | Short | Description |
|------|-------|-------------|
| `--node` | `-g` | Graph Node URL (required) |
| `--access-token` | | Access token for authentication |

### `gnd auth`

Store a deploy key for authentication.

```bash
gnd auth <DEPLOY_KEY>
```

**Flags:**

| Flag | Short | Description |
|------|-------|-------------|
| `--node` | `-g` | Graph Node URL (default: Subgraph Studio) |

Keys are stored in `~/.graph-cli.json`.

### `gnd test`

Run subgraph tests.

```bash
gnd test [TEST_FILES...]
```

**Arguments:**
- `PATHS`: Test JSON files or directories to scan. Defaults to `tests/` when nothing is specified.

**Flags:**

| Flag | Short | Description |
|------|-------|-------------|
| `--manifest` | `-m` | Path to subgraph manifest (default: `subgraph.yaml`) |
| `--skip-build` | | Skip building the subgraph before testing |
| `--postgres-url` | | PostgreSQL connection URL (env: `POSTGRES_URL`) |
| `--matchstick` | | Use legacy Matchstick runner |
| `--docker` | `-d` | Run Matchstick in Docker (requires `--matchstick`) |
| `--coverage` | `-c` | Run with coverage reporting (requires `--matchstick`) |
| `--recompile` | `-r` | Force recompilation (requires `--matchstick`) |
| `--force` | `-f` | Force redownload of Matchstick binary (requires `--matchstick`) |

**Examples:**

```bash
# Run all tests in tests/ directory (default)
gnd test

# Run specific test files
gnd test transfer.json approval.json
gnd test tests/transfer.json

# Scan a custom directory
gnd test my-tests/

# Use a different manifest
gnd test -m subgraph.staging.yaml tests/transfer.json

# Skip automatic build
gnd test --skip-build
```

### `gnd clean`

Remove build artifacts and generated files.

```bash
gnd clean
```

**Flags:**

| Flag | Short | Description |
|------|-------|-------------|
| `--codegen-dir` | | Codegen directory (default: `generated/`) |
| `--build-dir` | | Build directory (default: `build/`) |

### `gnd dev`

Run graph-node in development mode with file watching.

```bash
gnd dev [OPTIONS]
```

**Flags:**

| Flag | Short | Description |
|------|-------|-------------|
| `--watch` | | Watch build directory for changes |
| `--manifests` | | Subgraph manifest locations |
| `--sources` | | Source manifest locations for aliases |
| `--database-dir` | | Database directory (default: `./build`) |
| `--postgres-url` | | PostgreSQL connection URL |
| `--ethereum-rpc` | | Ethereum RPC URL |
| `--ipfs` | | IPFS node URL |

### `gnd completions`

Generate shell completions.

```bash
gnd completions <SHELL>
```

**Arguments:**
- `SHELL`: One of `bash`, `elvish`, `fish`, `powershell`, `zsh`

**Examples:**

```bash
# Bash
gnd completions bash > ~/.bash_completion.d/gnd

# Zsh
gnd completions zsh > ~/.zfunc/_gnd

# Fish
gnd completions fish > ~/.config/fish/completions/gnd.fish
```

## Configuration Files

### `~/.graph-cli.json`

Stores deploy keys for different Graph Node URLs. Created by `gnd auth`.

### `networks.json`

Network-specific configuration for contract addresses and start blocks:

```json
{
  "mainnet": {
    "MyContract": {
      "address": "0x1234...",
      "startBlock": 12345678
    }
  },
  "sepolia": {
    "MyContract": {
      "address": "0x5678...",
      "startBlock": 1000000
    }
  }
}
```

Use with `--network mainnet` on build/deploy commands.

## Differences from graph-cli

`gnd` is designed as a drop-in replacement for `graph-cli`. Some intentional differences:

### Commands Not Implemented

- **`local`**: Use graph-node's integration test infrastructure instead
- **`node`**: Use `graphman` for node management operations

### Code Generation Differences

These are documented and tested:

1. **Int8 import**: Always imported for simplicity, even when not used
2. **Trailing commas**: Used in multi-line constructs
3. **2D array accessors**: Uses correct `toStringMatrix()` (fixes a bug in graph-cli)

### Other Differences

- **Debug logging**: Uses `RUST_LOG` environment variable instead of `DEBUG=graph-cli:*`
- **`--uncrashable` flag**: Not implemented (Float Capital third-party feature)

## Environment Variables

| Variable | Description |
|----------|-------------|
| `RUST_LOG` | Debug logging level (e.g., `RUST_LOG=gnd=debug`) |
| `POSTGRES_URL` | PostgreSQL URL for `gnd dev` |
| `ETHEREUM_RPC` | Ethereum RPC URL for `gnd dev` |

## Version

```bash
gnd --version
# gnd 0.1.0 (graph-cli compatible: 0.98.1)
```

Shows both the gnd version and the graph-cli version it emulates.

## License

Apache-2.0 OR MIT
