#!/bin/sh
set -eu

case "${1:-test}" in
test)
	go test ./hat/hatDataStructure -run 'TestTT020' -count=1
	;;
package)
	go test ./hat/hatDataStructure -count=1
	;;
all)
	go test ./... -count=1
	;;
benchmark)
	go test ./hat/hatDataStructure -run '^$' -bench 'BenchmarkTT020' -benchmem -count=5
	;;
race)
	go test -race ./hat/hatDataStructure -run 'TestTT020' -count=1
	;;
vet)
	go vet ./hat/hatDataStructure
	;;
format)
	gofmt -w \
		hat/hatDataStructure/ordered_index.go \
		hat/hatDataStructure/tt020_range_test.go \
		hat/hatDataStructure/tt020_range_benchmark_test.go
	;;
*)
	printf 'usage: %s {test|package|all|benchmark|race|vet|format}\n' "$0" >&2
	exit 2
	;;
esac
