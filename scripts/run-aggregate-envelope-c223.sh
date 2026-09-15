#!/usr/bin/env bash
set -euo pipefail

mode="${1:-test}"
benchmark_time="${BENCHTIME:-250ms}"
benchmark_count="${BENCHCOUNT:-5}"
root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
workdir="$(mktemp -d "${TMPDIR:-/tmp}/hatrie-aggregate-envelope-c223.XXXXXX")"
trap 'rm -rf "$workdir"' EXIT

cp -a "$root/." "$workdir/"
rm -rf "$workdir/.git"
mkdir -p "$workdir/hat/hatSql"
git -C "$root" show HEAD:hat/hatSql/asof_join.go > "$workdir/hat/hatSql/asof_join.go"

cd "$workdir"
case "$mode" in
test)
	go test ./hat/hatDataStructure ./hat/hatCache -run 'Test(AggregateStateEnvelope|HyperLogLogAggregateState|CountMinSketchAggregateState)' -count=1
	;;
package)
	go test ./hat/hatDataStructure ./hat/hatCache -count=1
	;;
race)
	go test -race ./hat/hatDataStructure ./hat/hatCache -run 'Test(AggregateStateEnvelope|HyperLogLogAggregateState|CountMinSketchAggregateState)' -count=1
	;;
vet)
	go vet ./hat/hatDataStructure ./hat/hatCache
	;;
benchmark)
	go test ./hat/hatDataStructure ./hat/hatCache -run '^$' -bench 'AggregateState' -benchmem -benchtime="$benchmark_time" -count="$benchmark_count"
	;;
*)
	printf 'usage: %s {test|package|race|vet|benchmark}\n' "$0" >&2
	exit 2
	;;
esac
