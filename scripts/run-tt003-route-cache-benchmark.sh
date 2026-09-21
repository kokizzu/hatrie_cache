#!/usr/bin/env bash
set -euo pipefail

mode="${1:-final}"
case "$mode" in
	baseline)
		pattern='BenchmarkTT003ExistingRouteScan$'
		;;
	final)
		pattern='BenchmarkTT003'
		;;
	inplace)
		pattern='BenchmarkTT003RouteCacheLookup$'
		;;
	*)
		printf 'usage: %s [baseline|final|inplace]\n' "$0" >&2
		exit 2
		;;
esac

go test ./hat/hatReplication -run '^$' -bench "$pattern" -benchmem -count=5 -cpu=1
