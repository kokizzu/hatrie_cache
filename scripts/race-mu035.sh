#!/usr/bin/env bash
set -euo pipefail

cache_dir="$(mktemp -d /tmp/hatrie-cache-m035-race-cache.XXXXXX)"
cleanup() {
	rm -rf "$cache_dir"
}
trap cleanup EXIT

GOCACHE="$cache_dir" go test -race ./hat/hatSql -run 'TestSQLSnapshotReadiness' -count=1
