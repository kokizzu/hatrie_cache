#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT"

run_benchmark() {
	local cache
	cache=$(mktemp -d /tmp/hatrie-chu03-gocache.XXXXXX)
	trap "rm -rf '$cache'" EXIT
	for _ in 1 2 3 4 5; do
		GOCACHE="$cache" go test ./hat/hatDataStructure \
			-run '^$' \
			-bench '^BenchmarkCHU03' \
			-benchmem -benchtime=1s -count=1
	done
}

case "${1:-verify}" in
format)
	gofmt -w \
		hat/hatDataStructure/aggregate_state_registry.go \
		hat/hatDataStructure/aggregate_state_registry_test.go \
		hat/hatDataStructure/aggregate_state_registry_benchmark_test.go
	;;
test)
	go test ./hat/hatDataStructure
	;;
race)
	go test -race ./hat/hatDataStructure
	;;
vet)
	go vet ./hat/hatDataStructure
	;;
benchmark)
	run_benchmark
	;;
verify)
	bash "$0" format
	bash "$0" test
	bash "$0" race
	bash "$0" vet
	bash "$0" benchmark
	;;
*)
	echo "usage: $0 {format|test|race|vet|benchmark|verify}" >&2
	exit 2
	;;
esac
