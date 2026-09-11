#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatStorage -run '^$' -bench '^(BenchmarkCompactionSchedulerRun|BenchmarkCompactionSchedulerStats)$' -benchmem -benchtime=1s -count=5
