#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkCH037ColumnarArrayJoin(Baseline|Fallback|FastPath)$' -benchmem -count=5
