#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkM065abMutableRangeWindowBatchedSamePosition$' -benchmem -benchtime=100x -count=5
