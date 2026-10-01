#!/usr/bin/env bash
set -euo pipefail

mode="${1:-test}"
cache_dir="$(mktemp -d /tmp/hatrie-cache-chg11-gocache.XXXXXX)"
cleanup() {
	rm -rf "$cache_dir"
}
trap cleanup EXIT

case "$mode" in
format)
	gofmt -w hat/hatReplication/tu010_journal_write_quorum.go hat/hatReplication/tr011_journal_write_quorum_test.go
	;;
test)
	GOCACHE="$cache_dir" go test ./hat/hatReplication -run 'TestTU10JournalWriteQuorum' -count=1
	;;
package)
	GOCACHE="$cache_dir" go test ./hat/hatReplication -count=1
	;;
race)
	GOCACHE="$cache_dir" go test -race ./hat/hatReplication -count=1
	;;
vet)
	GOCACHE="$cache_dir" go vet ./hat/hatReplication
	;;
all)
	GOCACHE="$cache_dir" go test ./hat/hatReplication -count=1
	GOCACHE="$cache_dir" go test -race ./hat/hatReplication -count=1
	GOCACHE="$cache_dir" go vet ./hat/hatReplication
	;;
baseline)
	GOCACHE="$cache_dir" go test ./hat/hatReplication -run '^$' -bench 'WriteQuorum' -benchmem -benchtime=100ms -count=5
	;;
benchmark)
	GOCACHE="$cache_dir" go test ./hat/hatReplication -run '^$' -bench '^BenchmarkTU10JournalWriteQuorum$' -benchmem -benchtime=100ms -count=5
	;;
*)
	echo "usage: $0 {format|test|package|race|vet|all|baseline|benchmark}" >&2
	exit 2
	;;
esac
