#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLMutationDependency(Graph|Queue)' -benchmem -benchtime=100x -count=5
