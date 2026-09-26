#!/bin/sh
set -eu

cache_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-c153d-benchmark-cache.XXXXXX")
trap 'rm -rf "$cache_dir"' EXIT
output_file="$cache_dir/result.txt"
printf '%s\n' 'Running C153d benchmark'
if GOCACHE="$cache_dir" go test ./hat/hatTopology -run '^$' -bench '^BenchmarkC153dPartitionOwnershipConsensus$' -benchmem -count=5 -v >"$output_file" 2>&1; then
    status=0
else
    status=$?
fi
if [ "$status" -eq 0 ] && ! rg -q '^BenchmarkC153dPartitionOwnershipConsensus/(legacy|authenticated)-' "$output_file"; then
    printf '%s\n' 'C153d benchmark produced no matching benchmark rows' >&2
    status=1
fi
printf 'Benchmark output bytes: '
wc -c <"$output_file"
cat "$output_file"
exit "$status"
