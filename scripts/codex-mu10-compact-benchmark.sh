#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench 'BenchmarkDifferentialTemporalJoinAdaptiveCompact$|BenchmarkDifferentialTemporalJoinCompactionSchedulerCompactionTick$' -benchmem -count=3
