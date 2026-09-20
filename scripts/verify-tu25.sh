#!/usr/bin/env bash
set -euo pipefail

test -s TU25_RTREE_SPACE_CATALOG.md
rg -n -i 'T-U24|T-U25|R-tree space' README.md PRODUCT_IDEA_GAPS.md
rg -n -i 'T-U25|BenchmarkTU25CatalogRebuild' BENCHMARK.md
go test ./hat/hatDataStructure -count=1
go test -race ./hat/hatDataStructure -run '^TestTU25' -count=1
go vet ./hat/hatDataStructure
