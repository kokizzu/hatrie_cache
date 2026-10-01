#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure
go test -race ./hat/hatDataStructure -run 'TestUniqueConstraintSet' -count=1
go vet ./hat/hatDataStructure
test -s DATA_STRUCTURE.md
test -s TU22_CROSS_INDEX_UNIQUE_CONSTRAINTS.md
rg -q 'T-U22.*Adopted' PRODUCT_IDEA_GAPS.md
rg -q 'UniqueConstraintSet' README.md BENCHMARK.md TU22_CROSS_INDEX_UNIQUE_CONSTRAINTS.md
