#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatStorage/compaction_scheduler.go ./hat/hatStorage/compaction_scheduler_fastpath_benchmark_test.go -run '^$' -bench '^BenchmarkCompactionSchedulerRunC207$' -benchmem -count=3
