#!/usr/bin/env bash
set -euo pipefail

test -f hat/hatDataStructure/memtx_table.go
test -f TU16_MEMTX_TABLE.md
rg -q 'NewMemtxTable' hat/hatDataStructure/memtx_table.go TU16_MEMTX_TABLE.md README.md
rg -q 'T-U16' PRODUCT_IDEA_GAPS.md BENCHMARK.md
rg -q '6\.49x faster' TU16_MEMTX_TABLE.md BENCHMARK.md
git diff --check
git status --short
