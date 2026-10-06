#!/usr/bin/env bash
set -euo pipefail

gocache=/tmp/hatrie-ch-g32-bench-gocache-20261006
rm -rf "$gocache"
trap 'rm -rf "$gocache"' EXIT
GOCACHE="$gocache" go test ./hat/hatSql -run '^$' -bench '^BenchmarkCHG32ExistingIndexQueueEnqueueStatus$' -benchmem -benchtime=100ms -count=3
