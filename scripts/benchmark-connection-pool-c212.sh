#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatReplication -run '^$' -bench 'BenchmarkConnectionPoolAcquireRelease|BenchmarkConnectionDirectDialClose' -benchmem -count="${BENCH_COUNT:-5}"
