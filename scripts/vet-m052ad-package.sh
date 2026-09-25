#!/usr/bin/env bash
set -euo pipefail

cache_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-m052ad-vet-gocache.XXXXXX")
trap 'rm -rf "$cache_dir"' EXIT
GOCACHE="$cache_dir" go vet ./hat/hatSql
