#! /bin/bash

# This script is used by cloud build to push Docker images into Docker hub

tag_and_push() {
    tag=$1
    docker tag gcr.io/$PROJECT_ID/graph-node:$SHORT_SHA \
           lutter/graph-node:$tag
    docker push lutter/graph-node:$tag

    docker tag gcr.io/$PROJECT_ID/graph-node-debug:$SHORT_SHA \
           lutter/graph-node-debug:$tag
    docker push lutter/graph-node-debug:$tag
}

echo "Logging into Docker Hub"
echo $PASSWORD | docker login --username="$DOCKER_HUB_USER" --password-stdin

set -x

tag_and_push "$SHORT_SHA"
tag_and_push latest

if [ -n "$TAG_NAME" ]
then
    tag_and_push "$TAG_NAME"
fi
