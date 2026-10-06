#!/usr/bin/env bash
set -euo pipefail

cache_dir="$(mktemp -d /tmp/hatrie-m-g21-benchmark-gocache.XXXXXX)"
trap 'rm -rf "$cache_dir"' EXIT

GOCACHE="$cache_dir" go test ./hat/hatSql -run '^$' -bench '^BenchmarkTypedTableArrangementHydrationProgress/(baseline_hydrate|tracked_hydrate)$' -benchmem -benchtime=200ms -count=5
GOCACHE="$cache_dir" go test ./hat/hatSql -run '^$' -bench '^BenchmarkTypedTableArrangementHydration/(hydrate_one_change|rebuild_10000_changes)$' -benchmem -benchtime=100ms -count=3
