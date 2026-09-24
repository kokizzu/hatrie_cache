#!/usr/bin/env bash
set -euo pipefail

cache_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-cache-tt024-verify.XXXXXX")
trap 'rm -rf "$cache_dir"' EXIT

GOCACHE="$cache_dir" go test ./hat/hatSchema
GOCACHE="$cache_dir" go test -race ./hat/hatSchema -run '^TestTT024' -count=1
GOCACHE="$cache_dir" go vet ./hat/hatSchema
