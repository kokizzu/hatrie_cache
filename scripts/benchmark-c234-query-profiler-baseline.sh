#!/usr/bin/env bash
set -euo pipefail

cache_dir=$(mktemp -d /tmp/hatrie-c234-query-profiler-baseline-gocache.XXXXXX)
trap 'rm -rf "$cache_dir"' EXIT
GOCACHE="$cache_dir" go test ./hat/hatSql -run '^$' -bench '^BenchmarkC234QueryObservationBaseline$' -benchmem -benchtime=300ms -count=3
