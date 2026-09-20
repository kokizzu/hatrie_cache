#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatReplication/tu39_space_changefeed_baseline_benchmark_test.go \
  -run '^$' -bench '^BenchmarkTU39BaselineDirectEventAppend$' -benchmem -benchtime=1s -count=3
