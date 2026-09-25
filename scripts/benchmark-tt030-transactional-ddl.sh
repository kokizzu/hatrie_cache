#!/usr/bin/env bash
set -euo pipefail
cache=$(mktemp -d /tmp/hatrie-cache-tt030-after-XXXXXX)
trap 'rm -rf -- "$cache"' EXIT
GOCACHE="$cache" go test ./hat/hatSchema -run '^$' -bench '^BenchmarkTT030SpaceCatalogMutation$' -benchmem -count=5 -benchtime=100ms
