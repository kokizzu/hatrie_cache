#!/bin/sh
set -eu

GOCACHE_PATH=${GOCACHE:-/tmp/hatrie-cache-round59-gocache}
mkdir -p "$GOCACHE_PATH"
GOCACHE="$GOCACHE_PATH" go test ./hat/hatCache -run '^$' -bench '^BenchmarkDiskStorageGetReadPath$' -benchmem -benchtime=200ms -count=3
