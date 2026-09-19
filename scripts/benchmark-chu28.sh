#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatStorage -run '^$' -bench '^BenchmarkCHU28SchedulerRun(Baseline|Throttled)(Warm)?$' -benchmem -count=5
