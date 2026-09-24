#!/usr/bin/env bash
set -euo pipefail

cache_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-mz038-cursor-verify.XXXXXX")
trap 'rm -rf "$cache_dir"' EXIT
GOCACHE="$cache_dir" go test -race ./hat/hatSql -run '^TestMZ038SortedArrangementRange' -count=1
GOCACHE="$cache_dir" go vet ./hat/hatSql
