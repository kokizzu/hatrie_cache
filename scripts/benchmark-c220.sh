#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLQualifyAgainstSubquery$' -benchmem -count=5 -benchtime=1s
