#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkDifferentialTemporalJoinAdaptiveCompactCancellable$' -benchtime=200ms -benchmem -count=5
