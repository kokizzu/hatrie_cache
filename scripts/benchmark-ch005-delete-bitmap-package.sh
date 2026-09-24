#!/usr/bin/env bash
set -euo pipefail

cache_dir=$(mktemp -d /tmp/hatrie-cache-ch005-package-benchmark-cache.XXXXXX)
cleanup() {
	rm -rf "$cache_dir"
}
trap cleanup EXIT

GOCACHE="$cache_dir" go test ./hat/hatDataStructure -run '^$' -bench '^BenchmarkPersistentDeleteBitmapPacked(Encode|Decode)$' -benchmem -count=5
