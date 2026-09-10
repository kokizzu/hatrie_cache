#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLSourceFrontierBarrier(Baseline)?$' -benchmem -benchtime=250ms -count=5
