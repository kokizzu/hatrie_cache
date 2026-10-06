#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench 'BenchmarkDifferentialTemporalJoinAdaptive(Load|Recommendation)$|BenchmarkDifferentialTemporalJoinCompactionScheduler(IdleTick|CompactionTick)$' -benchmem -count=5
