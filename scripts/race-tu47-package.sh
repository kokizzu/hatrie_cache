#!/bin/sh
set -eu

cache_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-tu47-race-cache.XXXXXX")
trap 'rm -rf "$cache_dir"' EXIT
GOCACHE="$cache_dir" go test -race ./hat/hatReplication -count=1
