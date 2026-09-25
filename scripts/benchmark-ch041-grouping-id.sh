#!/usr/bin/env bash
set -euo pipefail

cache_dir="$(mktemp -d /tmp/hatrie-cache-bench-ch041-grouping-id.XXXXXX)"
trap 'rm -rf -- "$cache_dir"' EXIT
GOCACHE="$cache_dir" go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLGroupingIdentifier(Existing|Multiple)$' -benchmem -benchtime=200ms -count=5
