#!/usr/bin/env bash
set -euo pipefail

mode="${1:-test}"

case "$mode" in
test)
	go test ./hat/hatDataStructure -run '^TestDefaultValueColumn' -count=1
	;;
race)
	go test -race ./hat/hatDataStructure -count=1
	;;
benchmark)
	go test ./hat/hatDataStructure -run '^$' -bench '^BenchmarkDefaultValueColumn' -benchmem -benchtime="${BENCHTIME:-300ms}" -count="${COUNT:-5}"
	;;
*)
	printf 'usage: %s {test|race|benchmark}\n' "$0" >&2
	exit 2
	;;
esac
