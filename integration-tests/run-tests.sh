#!/bin/bash

set -e

cd ethereum-triggers
graph test "yarn test"
