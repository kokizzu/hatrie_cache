#!/usr/bin/env bash
set -euo pipefail
go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLRowBinaryCodecAccounting(Baseline)?$' -benchmem -count=5
