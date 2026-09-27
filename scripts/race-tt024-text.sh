#!/usr/bin/env bash
set -euo pipefail

cache_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-tt024-race.XXXXXX")
trap 'chmod -R u+w "$cache_dir" 2>/dev/null || true; rm -rf "$cache_dir"' EXIT
export GOCACHE="$cache_dir/go-build"
go test -race ./hat/hatSql -run '^TestSQLContainsPhrase' -count=1
