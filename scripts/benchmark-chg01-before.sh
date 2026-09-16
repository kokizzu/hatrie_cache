#!/bin/sh
set -eu

baseline=/tmp/hatrie-cache-chg01-baseline.n4Qm7R
output=/tmp/hatrie-cache-chg01-before.txt
if [ ! -d "$baseline" ]; then
    printf 'baseline worktree is missing: %s\n' "$baseline" >&2
    exit 1
fi

cd "$baseline"
go test ./hat/hatSql -run '^$' -bench '^BenchmarkCHG01' -benchmem -benchtime=100ms -count=5 > "$output"
cat "$output"
