#!/usr/bin/env bash
set -euo pipefail

mode=${1:-before}
benchmark='(BenchmarkSQLIndexAdvisor(PrimaryOrderRecommendations|PerFieldRecommendations|Covering)|BenchmarkCH023SQLIndexAdvisorPrimaryPrefixRecommendations)$'

case "$mode" in
before)
	workdir=$(mktemp -d)
	trap 'rm -rf "$workdir"' EXIT
	git archive HEAD | tar -x -C "$workdir"
	cd "$workdir"
	go test ./hat/hatSql -run '^$' -bench "$benchmark" -benchmem -count=5
	;;
after)
	go test ./hat/hatSql -run '^$' -bench "$benchmark" -benchmem -count=5
	;;
*)
	printf 'usage: %s [before|after]\n' "$0" >&2
	exit 2
	;;
esac
