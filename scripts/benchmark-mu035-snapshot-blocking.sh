#!/usr/bin/env bash
set -euo pipefail

cache_dir="$(mktemp -d /tmp/hatrie-mu035-bench-cache.XXXXXX)"
trap 'rm -rf "$cache_dir"' EXIT

printf '%s\n' '--- existing status baseline ---'
GOCACHE="$cache_dir" go test ./hat/hatPipeline -run '^$' -bench '^BenchmarkMU035SnapshotCutoverStatusReady$' -benchmem -count=5
printf '%s\n' '--- blocking wait ready fast path ---'
GOCACHE="$cache_dir" go test ./hat/hatPipeline -run '^$' -bench '^BenchmarkMU035SnapshotCutoverWaitReady$' -benchmem -count=5
