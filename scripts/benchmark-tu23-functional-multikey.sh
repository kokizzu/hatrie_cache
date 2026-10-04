#!/usr/bin/env bash
set -euo pipefail

ROOT=$(git rev-parse --show-toplevel)
CACHE=$(mktemp -d /tmp/hatrie-tu23-benchmark-cache.XXXXXX)
trap 'rm -rf "$CACHE"' EXIT

cd "$ROOT"
GOCACHE="$CACHE" go test ./hat/hatDataStructure/functional_multikey_contract_test \
	-run '^$' -bench 'BenchmarkTU23' -benchmem -count=5
