#!/usr/bin/env bash
set -euo pipefail

cache="${GOCACHE:-$PWD/.gocache}"
mkdir -p "$cache"
GOCACHE="$cache" go test ./hat/hatSql -run '^$' -bench '^BenchmarkTypedTableArrangementHydration/hydrate_one_change$|^BenchmarkM036AggregateHydrationStatus$' -benchmem -benchtime=100ms -count=1
