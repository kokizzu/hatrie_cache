#!/usr/bin/env bash
set -euo pipefail

mode="${1:-after}"
cache="$PWD/.codex-gocache-tu18"
mkdir -p "$cache"
case "$mode" in
baseline)
	benchmark='^BenchmarkTU18BaselineHatTrieBytes$'
	;;
after)
	benchmark='^BenchmarkTU18VolatileEngineBytes$'
	;;
*)
	printf 'usage: %s baseline|after\n' "$0" >&2
	exit 2
	;;
esac
GOCACHE="$cache" go test ./hat/hatCache -run '^$' -bench "$benchmark" -benchmem -count=5
