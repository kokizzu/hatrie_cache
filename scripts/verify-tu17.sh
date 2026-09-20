#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_root"
go test ./hat/hatDataStructure
go test -race ./hat/hatDataStructure -run '^TestTU17LSMTable'
go vet ./hat/hatDataStructure
test -f TU17_VINYL_LSM_TABLE.md
rg -q 'T-U17.*LSM' PRODUCT_IDEA_GAPS.md
rg -q 'T-U17 Selectable Vinyl-Style LSM Table' BENCHMARK.md
rg -q 'TU17_VINYL_LSM_TABLE.md' README.md
