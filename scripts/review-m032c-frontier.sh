#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestSQLSourceFrontierBarrier' -count=1
go vet ./hat/hatSql
rg -n 'SQLSourceFrontierBarrier|M032c|benchmark-m032c-frontier' \
	README.md SQL_SOURCE_FRONTIERS.md INSPIRATION.md CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md BENCHMARK.md
git diff --check
git status --short
