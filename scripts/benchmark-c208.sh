#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkExecuteSQLResultCache' -benchtime=1s -benchmem -count=5
