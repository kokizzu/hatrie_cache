#!/usr/bin/env bash
set -euo pipefail

mode="${1:-test}"
race_tmp=""
cleanup() {
	if [[ -n "$race_tmp" ]]; then
		rm -rf "$race_tmp"
	fi
}
trap cleanup EXIT

case "$mode" in
test)
	go test ./hat/hatCache -run '^TestTU06' -count=1
	;;
benchmark)
	go test ./hat/hatCache -run '^$' -bench '^BenchmarkTU06' -benchmem -count=5
	;;
race)
	race_tmp="$(mktemp -d /tmp/hatrie-cache-t-u06-race.XXXXXX)"
	mkdir -p "$race_tmp/cache" "$race_tmp/tmp"
	GOCACHE="$race_tmp/cache" GOTMPDIR="$race_tmp/tmp" go test -race ./hat/hatCache -run '^TestTU06' -count=1
	;;
vet)
	go vet ./hat/hatCache
	;;
format)
	gofmt -w hat/hatCache/replica_read_only.go hat/hatCache/tu06_replica_read_only_test.go hat/hatCache/tu06_replica_read_only_benchmark_test.go
	;;
package)
	go test ./hat/hatCache -count=1
	;;
*)
	printf 'usage: %s {test|benchmark|race|vet|format|package}\n' "$0" >&2
	exit 2
	;;
esac
