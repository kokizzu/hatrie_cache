#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkDifferentialTemporalJoin(Load|Adaptive)' -benchmem -count=5
