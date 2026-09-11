#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkCompiledSQLGroupedOrderedLimit(Baseline|Native)$' -benchmem -count=5
