#!/usr/bin/env bash
set -euo pipefail

cache_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-cache-tt034-go-cache.XXXXXX")
trap 'rm -rf "$cache_dir"' EXIT
GOCACHE="$cache_dir" go test ./hat/hatFiber -run '^$' -bench 'BenchmarkTU30(StacklessScheduler|GoroutineYieldControl)$' -benchmem -count=5
