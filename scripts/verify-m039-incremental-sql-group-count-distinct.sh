#!/usr/bin/env bash
set -euo pipefail

cache_dir=$(mktemp -d /tmp/hatrie-m039-verify-cache.XXXXXX)
trap 'rm -rf "$cache_dir"' EXIT

GOCACHE="$cache_dir" go test ./hat/hatSql
GOCACHE="$cache_dir" go test -race ./hat/hatSql -run '^TestM039SQLIncrementalGroupCountDistinct'
GOCACHE="$cache_dir" go vet ./hat/hatSql
