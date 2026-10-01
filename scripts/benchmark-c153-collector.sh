#!/usr/bin/env bash
set -euo pipefail

cache_dir=$(mktemp -d /tmp/hatrie-c153-collector-benchmark-cache.XXXXXX)
trap 'rm -rf "$cache_dir"' EXIT
GOCACHE="$cache_dir" go test ./hat/hatTopology -run '^$' -bench '^BenchmarkPartitionOwnershipConsensus$' -benchmem -benchtime=200ms -count=5
