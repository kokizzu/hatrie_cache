#!/usr/bin/env bash
set -euo pipefail

cache=$(mktemp -d /tmp/hatrie-tu39-go-cache.XXXXXX)
cleanup() {
	rm -rf -- "$cache"
}
trap cleanup EXIT

benchtime=${BENCH_TIME:-200ms}
count=${BENCH_COUNT:-3}
GOCACHE="$cache" go test ./hat/hatReplication \
	-run '^$' \
	-bench '^BenchmarkSpaceChangefeed' \
	-benchmem \
	-benchtime="$benchtime" \
	-count="$count"
