#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkCH045NestedMapSubcolumn$' -benchmem -benchtime=300ms -count=3
