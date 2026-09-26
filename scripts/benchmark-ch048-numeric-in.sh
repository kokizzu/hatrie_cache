#!/usr/bin/env bash
set -euo pipefail

cache_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-cache-ch048-numeric-in-bench.XXXXXX")
cleanup() {
	rm -rf "$cache_dir"
}
trap cleanup EXIT

GOCACHE="$cache_dir" go test ./hat/hatSql -run '^$' -bench '^BenchmarkCH048NumericIN(Baseline|Fallback|FastPath)$' -benchmem -benchtime=200ms -count=5
