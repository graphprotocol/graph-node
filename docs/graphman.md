# Graphman

## `graphman chain check-blocks`

### SYNOPSIS

    Compares cached blocks with fresh ones and clears the block cache when they differ

    USAGE:
        graphman --config <config> chain check-blocks <chain-name> <SUBCOMMAND>

    FLAGS:
        -h, --help       Prints help information
        -V, --version    Prints version information

    ARGS:
        <chain-name>    Chain name (must be an existing chain, see 'chain list')

    SUBCOMMANDS:
        by-hash      The number of the target block
        by-number    The hash of the target block
        by-range     A block number range, inclusive on both ends


<a id="org1502d78"></a>

### DESCRIPTION

The `check-blocks` command compares cached blocks with blocks from a JSON RPC provider and removes any blocks
from the cache that differ from the ones retrieved from the provider.

Sometimes JSON RPC providers send invalid block data to Graph Node. The `graphman chain check-blocks` command
is useful to diagnose the integrity of cached blocks and eventually fix them.


<a id="orga828a21"></a>

### OPTIONS

Blocks can be selected by different methods. The `check-blocks` command let's you use the block hash, a single
number or a number range to refer to which blocks it should verify:


<a id="orgc39ca48"></a>

#### `by-hash`

    graphman --config <config> chain check-blocks <chain-name> by-hash <hash>


<a id="orge321cf1"></a>

#### `by-number`

    graphman --config <config> chain check-blocks <chain-name> by-number <number> [--delete-duplicates]


<a id="org1506dd9"></a>

#### `by-range`

    graphman --config <config> chain check-blocks <chain-name> by-range [-f|--from <block-number>] [-t|--to <block-number>] [--delete-duplicates]

The `by-range` method lets you scan for numeric block ranges and offers the `--from` and `--to` options for
you to define the search bounds. If one of those options is ommited, `graphman` will consider an open bound
and will scan all blocks up to or after that number.

Over time, it can happen that a JSON RPC provider offers different blocks for the same block number. In those
cases, `graphman` will not decide which block hash is the correct one and will abort the operation. Because of
this, the `by-number` and `by-range` methods also provide a `--delete-duplicates` flag, which orients
`graphman` to delete all duplicated blocks for the given number and resume its operation.


<a id="org6b71975"></a>

### EXAMPLES

Inspect a single Ethereum Mainnet block by hash:

    graphman --config config.toml chain check-blocks mainnet by-hash 0xd56a9f64c7e696cfeb337791a7f4a9e81841aaf4fcad69f9bf2b2e50ad72b972

Inspect a block using its number:

    graphman --config config.toml chain check-blocks mainnet by-number 15626962

Inspect a block range, deleting any duplicated blocks:

    graphman --config config.toml chain check-blocks mainnet by-range --from 15626900 --to 15626962 --delete-duplicates

Inspect all blocks after block `13000000`:

    graphman --config config.toml chain check-blocks mainnet by-range --from 13000000
