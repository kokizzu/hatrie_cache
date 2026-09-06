#!/usr/bin/env bash
set -euo pipefail
go test -run '^$' -bench '^BenchmarkSQLAggregateCollections$' -benchmem ./hat/hatSql
