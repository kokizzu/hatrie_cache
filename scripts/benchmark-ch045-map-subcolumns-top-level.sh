#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkCH030MapSubcolumn$' -benchmem -benchtime=300ms -count=3
