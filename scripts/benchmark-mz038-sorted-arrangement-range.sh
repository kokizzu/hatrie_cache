#!/usr/bin/env bash
set -euo pipefail

cache_dir="$(mktemp -d "${TMPDIR:-/tmp}/hatrie-cache-mz038-range-benchmark.XXXXXX")"
cleanup() {
	rm -rf "$cache_dir"
}
trap cleanup EXIT

GOCACHE="$cache_dir" go test ./hat/hatSql -run '^$' -bench '^BenchmarkMZ038SortedArrangement(FullScanBaseline|OffsetPageBaseline|RowsRange)$' -benchmem -count=5
