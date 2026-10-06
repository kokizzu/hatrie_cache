#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' 'running existing journal benchmarks'
gocache=$(mktemp -d /tmp/hatrie-tu34-gocache.XXXXXX)
trap 'rm -rf "$gocache"' EXIT
GOCACHE="$gocache" go test -tags=tu34 ./hat/hatCache -run '^$' -bench '^BenchmarkTU34SpaceSyncPolicies$' -benchmem -benchtime=250ms -count=5
