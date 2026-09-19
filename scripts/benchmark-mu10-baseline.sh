#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkDifferentialTemporalJoin(Load|Compact)$' -benchmem -count=5
