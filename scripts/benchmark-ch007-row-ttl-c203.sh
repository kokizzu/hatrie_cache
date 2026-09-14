#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkCH007TypedTable(Rows|Cardinality)$' -benchmem -count=5
