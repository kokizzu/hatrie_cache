#!/bin/sh
set -eu

baseline=/tmp/hatrie-cache-chu50-baseline-c203
if [ -e "$baseline" ]; then
	printf '%s\n' "baseline worktree already exists: $baseline" >&2
	exit 1
fi
git worktree add --detach "$baseline" origin/master
cp hat/hatCache/ch_u50_backup_consistency_benchmark_test.go "$baseline/hat/hatCache/ch_u50_backup_consistency_benchmark_test.go"
printf '%s\n' "$baseline"
