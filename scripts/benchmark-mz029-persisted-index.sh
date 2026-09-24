#!/usr/bin/env bash
set -euo pipefail

cache_dir=$(mktemp -d /tmp/hatrie-cache-mz029-index-benchmark-cache.XXXXXX)
cleanup() {
	rm -rf "$cache_dir"
}
trap cleanup EXIT

GOCACHE="$cache_dir" go test ./hat/hatDataStructure -run '^$' -bench '^BenchmarkMZ029ReopenSpillableArrangement$' -benchmem -count=5
