#!/usr/bin/env bash
set -euo pipefail

echo "TT-049 row-lock benchmark"
go test ./hat/hatSql -run '^$' -bench 'BenchmarkTT049' -benchmem -count=5 -benchtime=100ms -v
