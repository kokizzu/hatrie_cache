#!/usr/bin/env sh
set -eu

go test ./hat/hatSql -run '^$' -bench '^BenchmarkAdaptivePlannerConcurrentFeedback$' -benchmem -count=5
