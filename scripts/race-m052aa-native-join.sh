#!/usr/bin/env bash
set -euo pipefail

cache_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-go-race-m052aa.XXXXXX")
trap 'rm -rf -- "$cache_dir"' EXIT
GOCACHE="$cache_dir" go test -race ./hat/hatSql -run '^TestM052AANativeJoin' -count=1
