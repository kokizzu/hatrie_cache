#!/usr/bin/env bash
set -euo pipefail

cache_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-go-cache-m033e-bench.XXXXXX")
tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-go-tmp-m033e-bench.XXXXXX")
trap 'rm -rf -- "$cache_dir" "$tmp_dir"' EXIT

GOCACHE="$cache_dir" GOTMPDIR="$tmp_dir" go test ./hat/hatReplication -run '^$' -bench 'BenchmarkGlobalTimestamp(OracleReserveOne|LeasePoolNext)$' -benchmem -count=5
