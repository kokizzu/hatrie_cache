#!/usr/bin/env bash
set -euo pipefail

set -o pipefail
go test ./hat/hatSql -run '^$' -bench '^BenchmarkM213' -benchmem -count=5 -benchtime=100ms 2>&1 | rg '^(goos|goarch|pkg:|cpu:|BenchmarkM213|PASS|ok[[:space:]])'
