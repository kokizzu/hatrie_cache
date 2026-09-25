#!/usr/bin/env bash
set -euo pipefail

cache_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-go-test-m052aa-package.XXXXXX")
trap 'rm -rf -- "$cache_dir"' EXIT
GOCACHE="$cache_dir" go test ./hat/hatSql -count=1
