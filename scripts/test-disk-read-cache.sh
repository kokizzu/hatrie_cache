#!/bin/sh
set -eu

GOCACHE_PATH=${GOCACHE:-/tmp/hatrie-cache-round59-gocache}
mkdir -p "$GOCACHE_PATH"
GOCACHE="$GOCACHE_PATH" go test ./hat/hatCache -run '^Test(DiskStorageReadCache|HatTrieDiskReadCache)' -count=1
