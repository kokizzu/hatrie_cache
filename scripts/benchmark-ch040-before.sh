#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLTDigestPercentile/gk-control$' -benchmem -count=5
