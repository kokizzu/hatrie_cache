#!/bin/sh
set -eu

case "${1:-}" in
format)
	gofmt -w hat/hatDataStructure/tu19_tuple_operation_journal.go hat/hatDataStructure/tt007_snapshot_wal_join_test.go hat/hatDataStructure/tt007_snapshot_wal_join_benchmark_test.go
	;;
test)
	go test ./hat/hatDataStructure -run '^TestTT007SnapshotJoin' -count=1
	;;
package)
	go test ./hat/hatDataStructure
	;;
race)
	go test -race ./hat/hatDataStructure -run '^TestTT007SnapshotJoin' -count=1
	;;
race-package)
	go test -race ./hat/hatDataStructure
	;;
benchmark)
	go test ./hat/hatDataStructure -run '^$' -bench '^BenchmarkTT007' -benchmem -count=5
	;;
review)
	git diff --cached --check
	git diff --cached --stat
	git diff --cached --name-only
	;;
vet)
	go vet ./hat/hatDataStructure
	;;
verify)
	bash "$0" format
	bash "$0" test
	bash "$0" race
	bash "$0" vet
	;;
*)
	printf '%s\n' 'usage: tt007-snapshot-wal-join.sh {format|test|package|race|race-package|benchmark|review|vet|verify}' >&2
	exit 2
	;;
esac
