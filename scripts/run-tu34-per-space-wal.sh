#!/usr/bin/env bash
set -euo pipefail

case "${1:-}" in
format)
	gofmt -w \
		api.go \
		hat/hatJournal/journal.go \
		hat/hatJournal/space_sync_policy.go \
		hat/hatJournal/tu34_per_space_sync_test.go \
		hat/hatCache/journal.go \
		hat/hatCache/tu34_per_space_sync_test.go \
		hat/hatCache/tu34_per_space_sync_benchmark_test.go
	;;
test)
	go test -count=1 -v ./hat/hatCache ./hat/hatJournal -run '^TestTU34'
	;;
full)
	go test -count=1 ./hat/hatCache ./hat/hatJournal
	;;
root)
	go test -count=1 .
	;;
all)
	go test -count=1 ./...
	;;
race)
	go test -race -count=1 -v ./hat/hatCache ./hat/hatJournal -run '^TestTU34'
	;;
vet)
	go vet ./hat/hatCache ./hat/hatJournal
	;;
benchmark)
	go test -count=1 -run '^$' -bench '^BenchmarkTR007GroupCommitFixed$|^BenchmarkTU34' -benchmem ./hat/hatCache
	;;
verify)
	bash "$0" format
	bash "$0" test
	bash "$0" race
	bash "$0" vet
	bash "$0" benchmark
	;;
*)
	echo "usage: $0 {format|test|full|root|all|race|vet|benchmark|verify}" >&2
	exit 2
	;;
esac
