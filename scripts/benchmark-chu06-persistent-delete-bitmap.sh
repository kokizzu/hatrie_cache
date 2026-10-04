#!/usr/bin/env bash
set -euo pipefail

ROOT=$(git rev-parse --show-toplevel)
CACHE=$(mktemp -d /tmp/hatrie-chu06-benchmark-cache.XXXXXX)
trap 'rm -rf "$CACHE"' EXIT

cd "$ROOT"
GOCACHE="$CACHE" go test ./hat/hatStorage/persistent_delete_bitmap_contract_test \
	-run '^$' -bench 'BenchmarkCHU06' -benchmem -count=5
