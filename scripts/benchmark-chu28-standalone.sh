#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatStorage/compaction_scheduler.go ./hat/hatStorage/compaction_scheduler_io.go ./hat/hatStorage/compaction_scheduler_stats.go ./hat/hatStorage/compaction_scheduler_fastpath_test.go ./hat/hatStorage/compaction_scheduler_fastpath_benchmark_test.go -run '^$' -bench '^BenchmarkCompactionSchedulerRunC207$' -benchmem -count="${BENCH_COUNT:-5}" -benchtime="${BENCH_TIME:-200ms}" "$@"
