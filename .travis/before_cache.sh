#! /bin/bash

# Remove test binaries and the artifacts for graph-node itself since
# they are pretty big and of no use in the cache. This is a game of
# whack-a-mole since there's no convenient way to have cargo tell us
# what corresponds to our subcrates directly, and what are dependencies
# of those

set -ev

cd $TRAVIS_BUILD_DIR/target/debug

du -hs .

find . -maxdepth 1 -type f -executable -printf '%f\n' \
    | xargs -iF rm -f F deps/F

rm -rf deps/*graph[_-]* .fingerprint/graph-* incremental/* ../.rustc_info.json

du -hs .
