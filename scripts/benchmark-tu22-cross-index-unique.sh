#!/usr/bin/env bash
set -euo pipefail

ROOT=$(git rev-parse --show-toplevel)
CACHE=$(mktemp -d /tmp/hatrie-tu22-benchmark-cache.XXXXXX)
trap 'rm -rf "$CACHE"' EXIT

cd "$ROOT"
GOCACHE="$CACHE" go test ./hat/hatDataStructure/cross_index_unique_contract_test \
	-run '^$' -bench 'BenchmarkTU22' -benchmem -count=5
