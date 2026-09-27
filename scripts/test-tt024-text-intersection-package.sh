#!/usr/bin/env bash
set -euo pipefail

cache_dir=$(mktemp -d /tmp/hatrie-cache-gocache-tt024-package.XXXXXX)
tmp_dir=$(mktemp -d /tmp/hatrie-cache-gotmp-tt024-package.XXXXXX)
trap 'rm -rf -- "$cache_dir" "$tmp_dir"' EXIT
GOCACHE="$cache_dir" GOTMPDIR="$tmp_dir" go test ./hat/hatSql ./hat/hatCache ./hat/hatSchema -count=1
