#!/bin/sh
set -eu

temporary_dir=$(mktemp -d /tmp/hatrie-chu51-group-by-all.XXXXXX)
cleanup() {
    rm -rf "$temporary_dir"
}
trap cleanup EXIT INT TERM

TMPDIR="$temporary_dir" GOCACHE="$temporary_dir/go-cache" go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLGroupByAll$' -benchmem -count=5
