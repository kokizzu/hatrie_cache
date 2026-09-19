#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatStorage -run '^$' -bench '^Benchmark(CompactionSchedulerRun|CHU35CompactionControllerRun)$' -benchmem -count=5 -benchtime=250ms
