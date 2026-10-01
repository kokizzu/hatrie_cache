#!/usr/bin/env bash
set -euo pipefail

cache_dir=$(mktemp -d /tmp/hatrie-cache-tu39-benchcache.XXXXXX)
trap 'rm -rf "$cache_dir"' EXIT

GOCACHE="$cache_dir" go test ./hat/hatReplication -run '^$' -bench 'BenchmarkSpaceChangefeed' -benchmem -count=5
