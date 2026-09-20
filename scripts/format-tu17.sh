#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_root"
gofmt -w \
	hat/hatDataStructure/lsm_table.go \
	hat/hatDataStructure/tu17_lsm_table_test.go \
	hat/hatDataStructure/tu17_lsm_baseline_benchmark_test.go \
	hat/hatDataStructure/tu17_lsm_table_benchmark_test.go
