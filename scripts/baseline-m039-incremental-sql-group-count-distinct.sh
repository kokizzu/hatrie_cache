#!/usr/bin/env bash
set -euo pipefail

cache_dir=$(mktemp -d /tmp/hatrie-m039-baseline-cache.XXXXXX)
trap 'rm -rf "$cache_dir"' EXIT
GOCACHE="$cache_dir" go test ./hat/hatSql -run '^$' -bench '^BenchmarkM039(RebuildSQLGroupCountDistinct|IncrementalSQLGroupCountDistinct)$' -benchtime=100ms -count=5
