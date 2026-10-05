#!/bin/sh
set -eu

tmp=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-chu53-select-star-except.XXXXXX")
trap 'rm -rf "$tmp"' EXIT

GOCACHE="$tmp/gocache" go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLSelectStarExcept$' -benchmem -count=5
