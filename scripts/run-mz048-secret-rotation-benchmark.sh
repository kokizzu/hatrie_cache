#!/usr/bin/env bash
set -euo pipefail

mode="${1:-final}"
case "$mode" in
	baseline)
		pattern='BenchmarkMZ048ExistingPauseResume$'
		;;
	final)
		pattern='BenchmarkMZ048'
		;;
	inplace)
		pattern='BenchmarkMZ048InPlaceRotation$'
		;;
	*)
		printf 'usage: %s [baseline|final|inplace]\n' "$0" >&2
		exit 2
		;;
esac

go test ./hat/hatPipeline -run '^$' -bench "$pattern" -benchmem -count=5 -cpu=1
