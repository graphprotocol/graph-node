#!/bin/bash

set -e
set -x

COMMIT=$(git rev-parse HEAD)
DOCKER_IMAGE="graph-node-test:$COMMIT"

pushd docker
docker build --build-arg SOURCE_BRANCH="$COMMIT" -t "$DOCKER_IMAGE" .
popd

pushd integration-tests

pushd ethereum-triggers
graph test --node-image "$DOCKER_IMAGE" "yarn test"
popd

popd
