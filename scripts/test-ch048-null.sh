#!/usr/bin/env bash
set -euo pipefail

cache_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-cache-ch048-null-test.XXXXXX")
cleanup() {
	rm -rf "$cache_dir"
}
trap cleanup EXIT

GOCACHE="$cache_dir" go test ./hat/hatSql -run '^TestCH048Null' -count=1
