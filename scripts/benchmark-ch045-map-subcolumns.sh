#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkCH(030MapSubcolumn|045NestedMapSubcolumn)$' -benchmem -benchtime=300ms -count=3
