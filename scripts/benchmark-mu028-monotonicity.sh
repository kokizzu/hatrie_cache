#!/usr/bin/env bash
set -euo pipefail

mode=${1:-current}
case "$mode" in
baseline)
	pattern='BenchmarkMU028SQLExpressionMonotonicity/baseline_parse/'
	;;
current)
	pattern='BenchmarkMU028SQLExpressionMonotonicity/analyze/'
	;;
*)
	printf 'usage: %s [baseline|current]\n' "$0" >&2
	exit 2
	;;
esac
go test ./hat/hatSql -run '^$' -bench "$pattern" -benchtime=1s -count=5
