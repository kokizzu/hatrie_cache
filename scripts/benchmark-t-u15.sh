#!/usr/bin/env bash
set -euo pipefail

output=$(mktemp)
trap 'rm -f "$output"' EXIT
go test ./hat/hatSchema -run '^$' -bench '^BenchmarkSpaceCatalogOperations$' -benchmem -count=5 >"$output"
rg '^BenchmarkSpaceCatalogOperations' "$output"
