#!/bin/bash

set -e

COMMIT=$(git rev-parse HEAD)

pushd docker
docker build .
popd

pushd integration-tests

pushd ethereum-triggers
graph test "yarn test"
popd

popd
