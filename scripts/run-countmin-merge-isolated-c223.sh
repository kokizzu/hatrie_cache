#!/usr/bin/env bash
set -euo pipefail

mode=${1:?mode is required}
repo=$(pwd)
work=$(mktemp -d /tmp/hatrie-cache-countmin-c223.XXXXXX)
trap 'rm -rf "$work"' EXIT

cp -a "$repo/." "$work/"
rm -rf "$work/.git"
mkdir -p "$work/hat/hatSql"
git -C "$repo" show HEAD:hat/hatSql/asof_join.go > "$work/hat/hatSql/asof_join.go"
cd "$work"

case "$mode" in
test)
	go test ./hat/hatCache -run 'Test(CountMinSketchMerge|HatTrieMergeCountMinSketch)' -count=1
	;;
package)
	go test ./hat/hatCache -count=1
	;;
race)
	go test -race ./hat/hatCache -run 'TestCountMinSketchMerge|TestHatTrieMergeCountMinSketch' -count=1
	;;
vet)
	go vet ./hat/hatCache
	;;
benchmark)
	go test ./hat/hatCache -run '^$' -bench '^BenchmarkCountMinSketchMergeAgainstReplay$' -benchmem -benchtime=200ms -count=3
	;;
*)
	printf 'unknown mode: %s\n' "$mode" >&2
	exit 2
	;;
esac
