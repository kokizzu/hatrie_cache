#!/usr/bin/env bash
set -euo pipefail

test -f hat/hatDataStructure/unique_constraints.go
test -f TU22_CROSS_INDEX_UNIQUENESS.md
rg -q 'UniqueConstraintSet' hat/hatDataStructure/unique_constraints.go TU22_CROSS_INDEX_UNIQUENESS.md README.md
rg -q 'T-U22' PRODUCT_IDEA_GAPS.md BENCHMARK.md
rg -q '2\.81x slower' TU22_CROSS_INDEX_UNIQUENESS.md BENCHMARK.md
git diff --check
git status --short
