#!/usr/bin/env bash
set -euo pipefail

cache=/tmp/hatrie-cache-t-u09-gocache
rm -rf "$cache"
mkdir -p "$cache"
trap 'rm -rf "$cache"' EXIT

GOCACHE="$cache" go test -race ./hat/hatReplication -run 'TestJoinBootstrap' -count=1
GOCACHE="$cache" go vet ./hat/hatReplication
