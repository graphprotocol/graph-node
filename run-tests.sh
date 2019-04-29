#!/bin/bash

set -e

COMMIT=$(git rev-parse HEAD)
DOCKER_IMAGE="graph-node-test:$COMMIT"

echo "COMMIT=$COMMIT"
echo "DOCKER_IMAGE=$DOCKER_IMAGE"

cd docker
docker build --build-arg SOURCE_BRANCH="$COMMIT" -t "$DOCKER_IMAGE" .
cd ..

cd integration-tests

cd ethereum-triggers
graph test --node-image "$DOCKER_IMAGE" "yarn test"
cd ..

cd dynamic-data-sources
graph test --node-image "$DOCKER_IMAGE" "yarn test"
cd ..

cd ..
