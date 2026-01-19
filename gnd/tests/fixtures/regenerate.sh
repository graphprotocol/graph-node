#!/usr/bin/env bash
#
# Regenerate codegen fixtures using graph-cli.
#
# This script regenerates the `generated/` directories in the codegen_verification
# fixtures using the `graph` CLI. Run this when you need to update the expected
# output after changes to graph-cli codegen.
#
# Prerequisites:
#   - graph CLI must be installed (`pnpm install -g @graphprotocol/graph-cli`)
#   - Or use npx: GRAPH_CMD="npx graph" ./regenerate.sh
#
# Usage:
#   ./regenerate.sh              # Regenerate all fixtures
#   ./regenerate.sh fixture-name # Regenerate specific fixture

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FIXTURES_DIR="$SCRIPT_DIR/codegen_verification"

# Allow overriding the graph command (e.g., GRAPH_CMD="npx graph")
GRAPH_CMD="${GRAPH_CMD:-graph}"

FIXTURES=(
    "2d-array-is-valid"
    "3d-array-is-valid"
    "big-decimal-is-valid"
    "block-handler-filters"
    "call-handler-with-tuple"
    "derived-from-with-interface"
    "example-values-found"
    "invalid-graphql-schema"
    "no-network-names"
    "source-without-address-is-valid"
    "topic0-is-valid"
)

# Check if graph CLI is available
if ! command -v $GRAPH_CMD &> /dev/null; then
    echo "Error: '$GRAPH_CMD' command not found."
    echo ""
    echo "Install graph-cli with one of these methods:"
    echo "  pnpm install -g @graphprotocol/graph-cli"
    echo "  npm install -g @graphprotocol/graph-cli"
    echo ""
    echo "Or use npx to run without installing:"
    echo "  GRAPH_CMD=\"npx graph\" $0"
    exit 1
fi

regenerate_fixture() {
    local fixture="$1"
    local fixture_path="$FIXTURES_DIR/$fixture"

    if [[ ! -d "$fixture_path" ]]; then
        echo "Error: Fixture directory not found: $fixture_path"
        return 1
    fi

    echo "Regenerating: $fixture"

    # Remove existing generated directory
    rm -rf "$fixture_path/generated"

    # Run graph codegen
    pushd "$fixture_path" > /dev/null
    if $GRAPH_CMD codegen --skip-migrations 2>/dev/null; then
        echo "  OK"
    else
        echo "  FAILED (this may be expected for invalid fixtures)"
    fi
    popd > /dev/null
}

if [[ $# -gt 0 ]]; then
    # Regenerate specific fixture(s)
    for fixture in "$@"; do
        regenerate_fixture "$fixture"
    done
else
    # Regenerate all fixtures
    echo "Regenerating all codegen fixtures..."
    echo ""
    for fixture in "${FIXTURES[@]}"; do
        regenerate_fixture "$fixture"
    done
    echo ""
    echo "Done! Review changes with: git diff gnd/tests/fixtures/"
fi
