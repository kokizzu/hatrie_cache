#!/usr/bin/env bash
set -euo pipefail
go test ./hat/hatSql -run '^$' -bench '^BenchmarkMZ037TopK(RebuildBaseline|Incremental)$' -benchmem -count=5
