#!/usr/bin/env bash
set -euo pipefail

cache_dir=$(mktemp -d /tmp/hatrie-cache-mz028-benchmark-cache.XXXXXX)
cleanup() {
	rm -rf "$cache_dir"
}
trap cleanup EXIT
GOCACHE="$cache_dir" go test ./hat/hatSql -run '^$' -bench '^BenchmarkMZ028BatchedNewGroups$' -benchmem -count=5
