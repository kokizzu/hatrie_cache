#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkM208(SortFoldBaseline|HashFold)$' -benchmem -benchtime=2s -count=5
