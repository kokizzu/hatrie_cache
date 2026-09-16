#!/bin/sh
set -eu

mode=${1:-after}
baseline=/tmp/hatrie-cache-chu50-baseline-c203
case "$mode" in
	before)
		worktree=$baseline
		output=/tmp/hatrie-cache-chu50-before-c203.txt
		;;
	after)
		worktree=$(pwd)
		output=/tmp/hatrie-cache-chu50-after-c203.txt
		;;
	*)
		printf '%s\n' "usage: $0 before|after" >&2
		exit 2
		;;
esac

if [ ! -d "$worktree" ]; then
	printf '%s\n' "benchmark worktree does not exist: $worktree" >&2
	exit 1
fi
(cd "$worktree" && go test ./hat/hatCache -run '^$' -bench '^BenchmarkCHU50Backup' -benchmem -benchtime=100ms -count=5) > "$output"
cat "$output"
