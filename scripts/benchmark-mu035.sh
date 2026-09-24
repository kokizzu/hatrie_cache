#!/usr/bin/env bash
set -euo pipefail

cache_dir="$(mktemp -d /tmp/hatrie-cache-m035-benchmark-cache.XXXXXX)"
cleanup() {
	rm -rf "$cache_dir"
}
trap cleanup EXIT

GOCACHE="$cache_dir" go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLSnapshotReadiness' -benchmem -count=5
