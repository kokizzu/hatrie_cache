#!/usr/bin/env bash
set -euo pipefail

cache_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-cache-ch048-null-vet.XXXXXX")
cleanup() {
	rm -rf "$cache_dir"
}
trap cleanup EXIT

GOCACHE="$cache_dir" go vet ./hat/hatSql
