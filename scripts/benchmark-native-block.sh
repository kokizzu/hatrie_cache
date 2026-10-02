#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLNativeBlockVsRowBinary$' -benchmem -count=5 -benchtime=200ms
