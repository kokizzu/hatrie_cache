#!/usr/bin/env bash
set -euo pipefail

go test \
  hat/hatStorage/remote_part.go \
  hat/hatStorage/remote_part_cache.go \
  hat/hatStorage/remote_part_cache_c243_benchmark_test.go \
  hat/hatStorage/remote_part_cache_c245_benchmark_test.go \
  -run '^$' -bench '^BenchmarkC(243|245)RemotePartCache' -benchmem -benchtime=250ms -count=5
