#!/usr/bin/env bash
set -euo pipefail
go test -run '^$' -bench '^BenchmarkSQLArrayJoin$' -benchmem ./hat/hatSql
