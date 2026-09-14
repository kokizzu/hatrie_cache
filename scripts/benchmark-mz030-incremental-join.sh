#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkMZ030' -benchmem -count=5 -benchtime="${BENCHTIME:-1s}"
