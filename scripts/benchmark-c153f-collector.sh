#!/usr/bin/env bash
set -euo pipefail

tmp_output=$(mktemp /tmp/hatrie-c153f-benchmark.XXXXXX)
cleanup() {
	rm -f -- "$tmp_output"
}
trap cleanup EXIT
go test ./hat/hatTopology -run '^$' -bench '^BenchmarkPartitionOwnershipConsensusCollection(Baseline)?$' -benchmem -count=5 > "$tmp_output" 2>&1
awk '/BenchmarkPartitionOwnershipConsensusCollection/ {print}' "$tmp_output"
