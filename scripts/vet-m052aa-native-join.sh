#!/usr/bin/env bash
set -euo pipefail

cache_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-go-vet-m052aa.XXXXXX")
trap 'rm -rf -- "$cache_dir"' EXIT
GOCACHE="$cache_dir" go vet ./hat/hatSql
