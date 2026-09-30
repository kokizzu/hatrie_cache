#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkRound16ColumnarLimitWithTies$' -benchmem -benchtime=250ms -count=5
