#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkCompiledSQLGroupedHavingOrderedLimit(Baseline|Native)$' -benchmem -count=5
