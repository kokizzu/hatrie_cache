#!/usr/bin/env bash
set -euo pipefail

cache_dir=$(mktemp -d /tmp/hatrie-go-cache-t047j-benchmark.XXXXXX)
cleanup() {
	rm -rf "$cache_dir"
}
trap cleanup EXIT

GOCACHE="$cache_dir" go test ./hat/hatCache -run '^$' -bench '^BenchmarkTU047(HTTPTransport|DirectCoordinatorBaseline)$' -benchmem -count=3
