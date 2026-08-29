#!/usr/bin/env sh
set -eu

go test ./hat/hatCache -run '^$' -bench '^BenchmarkCompactMemoryAdmission10k$' -benchmem -count=5
