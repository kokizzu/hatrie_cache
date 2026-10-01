#!/bin/sh
set -eu

mode=${1:-test}
cache_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-cache-chg10-test.XXXXXX")
trap 'rm -rf "$cache_dir"' EXIT HUP INT TERM

case "$mode" in
test)
	GOCACHE="$cache_dir" go test ./hat/hatReplication -run '^TestTU06ReplicaReadOnlyGate' -count=1
	;;
format)
	gofmt -w hat/hatReplication/tr010_replica_read_only_gate.go hat/hatReplication/tr010_replica_read_only_gate_test.go
	;;
benchmark)
	GOCACHE="$cache_dir" go test ./hat/hatReplication -run '^$' -bench '^BenchmarkTU06ReplicaReadOnlyGate$' -benchmem -count="${BENCH_COUNT:-5}"
	;;
*)
	printf 'unknown mode: %s\n' "$mode" >&2
	exit 2
	;;
esac
