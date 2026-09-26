#!/bin/sh
set -eu

cache_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-c153e-bench-cache.XXXXXX")
output_file=$(mktemp "${TMPDIR:-/tmp}/hatrie-c153e-bench-output.XXXXXX")
trap 'rm -rf "$cache_dir" "$output_file"' EXIT HUP INT TERM

GOCACHE="$cache_dir" go test ./hat/hatTopology -run '^$' -bench '^BenchmarkC153ePartitionOwnershipVoteWire(JSON)?$' -benchmem -count=5 -v | tee "$output_file"
rg -q '^BenchmarkC153ePartitionOwnershipVoteWireJSON/(marshal|unmarshal)-' "$output_file"
rg -q '^BenchmarkC153ePartitionOwnershipVoteWire/(marshal|unmarshal)-' "$output_file"
