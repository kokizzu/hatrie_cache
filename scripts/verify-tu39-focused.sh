#!/usr/bin/env bash
set -euo pipefail

cache_dir=$(mktemp -d /tmp/hatrie-cache-tu39-gocache.XXXXXX)
trap 'rm -rf "$cache_dir"' EXIT

GOCACHE="$cache_dir" go test ./hat/hatReplication -count=1
GOCACHE="$cache_dir" go test -race ./hat/hatReplication -run 'TestSpaceChangefeed' -count=1
GOCACHE="$cache_dir" go vet ./hat/hatReplication
