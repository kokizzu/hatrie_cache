#!/bin/sh
set -eu

baseline=/tmp/hatrie-cache-chu50-baseline-c203
if [ ! -d "$baseline" ]; then
	printf '%s\n' "baseline worktree does not exist: $baseline" >&2
	exit 1
fi
cp hat/hatCache/ch_u50_backup_consistency_benchmark_test.go "$baseline/hat/hatCache/ch_u50_backup_consistency_benchmark_test.go"
