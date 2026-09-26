#!/usr/bin/env bash
set -euo pipefail
cache_dir="$(mktemp -d "${TMPDIR:-/tmp}/hatrie-mz009-go-build.XXXXXX")"
trap 'rm -rf "$cache_dir"' EXIT
GOCACHE="$cache_dir" go test ./hat/hatSql
