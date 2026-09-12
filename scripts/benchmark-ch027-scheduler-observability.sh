#!/bin/sh
set -eu

go test ./hat/hatStorage -run '^$' -bench '^BenchmarkCH027SchedulerStats' -benchmem -count=5
