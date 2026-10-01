#!/bin/sh
set -eu

mode=${1:-test}
cache_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-cache-chg08-test.XXXXXX")
trap 'rm -rf "$cache_dir"' EXIT HUP INT TERM

case "$mode" in
test)
	GOCACHE="$cache_dir" go test ./hat/hatReplication -run '^TestTU09SnapshotWALBootstrap' -count=1
	;;
package)
	GOCACHE="$cache_dir" go test ./hat/hatReplication -count=1
	;;
all)
	GOCACHE="$cache_dir" go test ./... -count=1
	;;
race)
	GOCACHE="$cache_dir" go test -race ./hat/hatReplication -count=1
	;;
vet)
	GOCACHE="$cache_dir" go vet ./hat/hatReplication
	;;
format)
	gofmt -w hat/hatReplication/tr009_snapshot_wal_bootstrap.go hat/hatReplication/tr009_snapshot_wal_bootstrap_test.go
	;;
benchmark)
	GOCACHE="$cache_dir" go test ./hat/hatReplication -run '^$' -bench '^BenchmarkTU09SnapshotWALBootstrap$' -benchmem -count="${BENCH_COUNT:-5}"
	;;
*)
	printf 'unknown mode: %s\n' "$mode" >&2
	exit 2
	;;
esac
