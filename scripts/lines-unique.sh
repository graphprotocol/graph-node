#!/bin/bash

source 'scripts/abort.sh'

# Exits with code 0 if lines are unique, 1 otherwise
LINES=$@

# Validate that parameters are being sent
[ -z "$LINES" ] && abort "No lines received"

if [ $(echo $LINES | tr " " "\n" | sort | uniq | wc -l) -eq 1 ]; then
  exit 0
else
  exit 1
fi
