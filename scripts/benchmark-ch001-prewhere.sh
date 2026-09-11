#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLExplicitPrewhere(Baseline|Fallback|FallbackBaseline)?$' -benchmem -count=5
