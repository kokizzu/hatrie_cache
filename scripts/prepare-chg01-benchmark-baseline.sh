#!/bin/sh
set -eu

baseline=/tmp/hatrie-cache-chg01-baseline.n4Qm7R
if [ -e "$baseline" ]; then
    printf 'baseline worktree already exists: %s\n' "$baseline" >&2
    exit 1
fi

git worktree add --detach "$baseline" origin/master
cp hat/hatSql/chg01_external_group_spill_benchmark_test.go "$baseline/hat/hatSql/chg01_external_group_spill_benchmark_test.go"
printf 'prepared %s\n' "$baseline"
