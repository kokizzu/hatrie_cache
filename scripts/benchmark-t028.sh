#!/usr/bin/env bash
set -u

cache=$(mktemp -d /tmp/hatrie-t028-feature-benchmark-cache.XXXXXX)
tmp=$(mktemp -d /tmp/hatrie-t028-feature-benchmark-tmp.XXXXXX)
cleanup() {
	rm -rf "$cache" "$tmp"
}
trap cleanup EXIT

GOMAXPROCS=1 GOCACHE="$cache" GOTMPDIR="$tmp" go test ./hat/hatPeer -run '^$' -bench '^BenchmarkT028ConnectionPoolLifecycleNotify$' -benchmem -cpu=1 -count=5
