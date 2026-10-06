#!/usr/bin/env bash
set -euo pipefail

cache_dir="$(mktemp -d /tmp/hatrie-c237-benchmark-cache.XXXXXX)"
trap 'rm -rf "$cache_dir"' EXIT

GOCACHE="$cache_dir" go test ./hat/hatSql -run '^$' -bench '^(BenchmarkCostedExplainMZ044|BenchmarkProjectionCatalog)' -benchmem -count=5
