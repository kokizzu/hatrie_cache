#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatStorage -run '^$' -bench '^BenchmarkCompactionSchedulerRun$' -benchmem -count=5 -benchtime=250ms
