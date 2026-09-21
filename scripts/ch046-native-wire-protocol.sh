#!/usr/bin/env bash
set -euo pipefail

mode=${1:-test}

case "$mode" in
test)
	go test ./hat/hatSql -run '^TestSQLColumnarBlockStream' -count=1
	go test ./hat/hatCache -run '^TestMonitoringSQLRoute(StreamsColumnarBlocks|DoesNotSelectDisabledColumnar)$' -count=1
	;;
race)
	go test -race ./hat/hatSql -run '^TestSQLColumnarBlockStream' -count=1
	go test -race ./hat/hatCache -run '^TestMonitoringSQLRoute(StreamsColumnarBlocks|DoesNotSelectDisabledColumnar)$' -count=1
	;;
package)
	go test ./hat/hatSql
	go test ./hat/hatCache
	;;
cache-package)
	go test ./hat/hatCache
	;;
vet)
	go vet ./hat/hatSql ./hat/hatCache
	;;
size)
	go test -v ./hat/hatCache -run '^TestCH046WireSizes$' -count=1
	;;
benchmark)
	go test ./hat/hatCache -run '^$' -bench '^BenchmarkCH046' -benchmem -count=5
	;;
dictionary-benchmark)
	go test ./hat/hatSql -run '^$' -bench '^BenchmarkCH046WireDictionary' -benchmem -count=5
	;;
baseline)
	go test -v ./hat/hatCache -run '^$' -bench 'Baseline' -benchmem -count=5
	;;
format)
	gofmt -w hat/hatSql/ch046_native_wire_protocol_test.go hat/hatSql/ch046_wire_dictionary_test.go hat/hatSql/ch046_wire_dictionary_benchmark_test.go hat/hatSql/columnar_block_stream.go
	;;
*)
	printf 'usage: %s {test|race|package|cache-package|vet|size|benchmark|dictionary-benchmark|baseline|format}\n' "$0" >&2
	exit 2
	;;
esac
