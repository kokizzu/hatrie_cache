#!/usr/bin/env bash
set -euo pipefail

cache_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-m052ad-bench-gocache.XXXXXX")
trap 'rm -rf "$cache_dir"' EXIT
GOCACHE="$cache_dir" go test ./hat/hatSql -run '^$' -bench '^BenchmarkCompiledSQLAutomaticNativeConditionalAggregates$' -benchmem -count=3
