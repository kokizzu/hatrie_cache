#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_root"
go test ./hat/hatDataStructure -run '^$' -bench '^(Benchmark(String|Tuple)MultikeyIndex)' -benchmem -count=5
