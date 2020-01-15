#!/bin/bash
#
# Usage:   ./run-tests [<test-name>]
# Example: ./run-tests overloaded-contract-functions

set -e

TEST="$1"

echo
echo "------------------------------------------------------------------------"
echo " Build Graph Node"
echo "========================================================================"
echo

cargo build -p graph-node

cd tests

if [ -z "$TEST" ] || [ "$TEST" = "overloaded-contract-functions" ]; then
    echo
    echo "------------------------------------------------------------------------"
    echo " Overloaded contract functions"
    echo "========================================================================"
    echo

    cd overloaded-contract-functions
    yarn
    ./node_modules/.bin/graph test --standalone-node ../../target/debug/graph-node "yarn test"
    cd ..
fi

cd ..
