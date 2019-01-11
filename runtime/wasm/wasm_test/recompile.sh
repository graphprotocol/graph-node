#!/bin/bash
# Re-compiles all tests. `asc` must be installed.

for i in *.ts; do
    asc $i -b $(basename "$i" .ts).wasm --validate
done
