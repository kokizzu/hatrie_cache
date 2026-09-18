#!/usr/bin/env bash
set -euo pipefail

echo "MZ024 arrangement-selection benchmark"
go test ./hat/hatSql -run '^$' -bench '^BenchmarkMZ024ArrangementSelection$' -benchmem -count=5 -benchtime=100ms -v
