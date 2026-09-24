#!/usr/bin/env bash
set -euo pipefail

cache="${TMPDIR:-/tmp}/hatrie-cache-mu034-benchmark-gocache"
rm -rf "$cache"
mkdir -p "$cache"
trap 'rm -rf "$cache"' EXIT
GOCACHE="$cache" go test ./hat/hatSql -run '^$' -bench 'BenchmarkMU034HistoricalSubscriptionCheckpoint' -benchmem -count=5 -benchtime=200ms
