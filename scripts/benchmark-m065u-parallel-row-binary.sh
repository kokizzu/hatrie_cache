#!/usr/bin/env bash
set -euo pipefail

GOMAXPROCS=8 go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLRowBinaryParallelDecodeWorkload$' -benchmem -benchtime=250ms -count=7
