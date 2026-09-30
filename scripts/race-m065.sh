#!/usr/bin/env bash
set -euo pipefail

cache_dir=/tmp/hatrie-cache-m065-race-gocache
mkdir -p "$cache_dir"
trap 'rm -rf "$cache_dir"' EXIT
GOCACHE="$cache_dir" go test -race ./hat/hatSql -run '^TestM065' -count=1
