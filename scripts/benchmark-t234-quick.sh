#!/usr/bin/env bash
set -euo pipefail

output=.t234-benchmark-quick-output
trap 'rm -f "$output"' EXIT
go test ./hat/hatDataStructure -run '^$' -bench '^BenchmarkT234(RegularTransactionWriteVinylBaseline|ConflictTransactionWriteVinyl|ConflictTransactionRejected)$' -benchmem -count=3 >"$output"
rg '^(goos|goarch|pkg|cpu|Benchmark|PASS|ok)' "$output"
