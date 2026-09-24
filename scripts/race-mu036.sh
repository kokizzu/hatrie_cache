#!/usr/bin/env bash
set -euo pipefail

cache_dir="$(mktemp -d /tmp/hatrie-cache-m036-race-cache.XXXXXX)"
cleanup() {
	rm -rf "$cache_dir"
}
trap cleanup EXIT

GOCACHE="$cache_dir" go test -race ./hat/hatSql -run 'TestTypedTable.*Hydration(Status|Failure|Context)|TestTypedTableArrangementHydration' -count=1
