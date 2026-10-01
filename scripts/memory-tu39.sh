#!/usr/bin/env bash
set -euo pipefail

cache_dir=$(mktemp -d /tmp/hatrie-cache-tu39-memorycache.XXXXXX)
trap 'rm -rf "$cache_dir"' EXIT

GOCACHE="$cache_dir" go test ./hat/hatReplication -run '^TestSpaceChangefeedRetainedMemory$' -count=1 -v
