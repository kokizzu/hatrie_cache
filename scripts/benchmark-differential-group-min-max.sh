#!/usr/bin/env bash
set -euo pipefail

cache_dir="$(mktemp -d "${TMPDIR:-/tmp}/hatrie-cache-bench.XXXXXX")"
trap 'rm -rf "$cache_dir"' EXIT
results="$cache_dir/results.txt"
printf '%s\n' 'benchmark-start'
if ! GOCACHE="$cache_dir" go test -v ./hat/hatSql -run '^$' -bench '^Benchmark(DifferentialGroupMinMax|M037kDifferentialStringMinMax|M037lDifferentialStringCountDistinct)/(naive_rebuild|incremental)$' -benchmem -count=5 -cpu=1 >"$results" 2>&1; then
	printf '%s\n' 'benchmark-failed'
	sed -n '1,260p' "$results"
	exit 1
fi
printf 'benchmark-bytes: '
wc -c "$results"
sed -n '1,260p' "$results"
printf '%s\n' 'benchmark-end'
