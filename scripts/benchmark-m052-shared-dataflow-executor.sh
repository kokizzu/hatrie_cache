#!/usr/bin/env bash
set -euo pipefail

output=$(mktemp)
trap 'rm -f "$output"' EXIT
go test ./hat/hatSql -run '^$' -bench '^BenchmarkM052DataflowExecutorCompile$' -benchmem -count=5 >"$output"
rg '^(BenchmarkM052DataflowExecutorCompile|PASS|ok[[:space:]])' "$output"
