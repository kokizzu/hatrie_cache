#!/usr/bin/env bash
set -euo pipefail

mode="${1:-current}"
case "$mode" in
baseline)
	benchmark='BenchmarkMU027Before'
	;;
current)
	benchmark='BenchmarkMU027'
	;;
*)
	printf 'usage: %s [baseline|current]\n' "$0" >&2
	exit 2
	;;
esac

GOMAXPROCS=1 go test ./hat/hatSql -run '^$' -bench "$benchmark" -benchmem -benchtime=500ms -count=5
