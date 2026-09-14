#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkMZ039Distinct' -benchmem -count=5 -benchtime="${BENCHTIME:-1s}"
