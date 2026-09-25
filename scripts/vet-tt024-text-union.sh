#!/usr/bin/env bash
set -euo pipefail

cache_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-cache-tt024-union-vet.XXXXXX")
trap 'rm -rf "$cache_dir"' EXIT

GOCACHE="$cache_dir" go vet ./hat/hatSql ./hat/hatCache ./hat/hatSchema
