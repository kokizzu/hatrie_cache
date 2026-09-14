#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkC212TypedTableOrder(ColumnarTopNBaseline|CachedProjection)$' -benchmem -benchtime=1s -count=5
