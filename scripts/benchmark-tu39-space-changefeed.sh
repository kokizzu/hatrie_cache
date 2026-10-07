#!/usr/bin/env bash
set -euo pipefail

action=${1:-benchmark}

case "$action" in
benchmark)
	cache=$(mktemp -d /tmp/hatrie-tu39-go-cache.XXXXXX)
	cleanup() {
		rm -rf -- "$cache"
	}
	trap cleanup EXIT

	benchtime=${BENCH_TIME:-100ms}
	count=${BENCH_COUNT:-5}
	GOCACHE="$cache" go test ./hat/hatReplication \
		-run '^$' \
		-bench '^BenchmarkSpaceChangefeed' \
		-benchmem \
		-benchtime="$benchtime" \
		-count="$count"
	;;
test)
	go test ./hat/hatReplication -run '^TestSpaceChangefeed' -count=1
	;;
race)
	go test ./hat/hatReplication -race -run '^TestSpaceChangefeed' -count=1
	;;
vet)
	go vet ./hat/hatReplication
	;;
*)
	echo "usage: $0 {benchmark|test|race|vet}" >&2
	exit 2
	;;
esac
