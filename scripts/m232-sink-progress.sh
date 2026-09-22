#!/usr/bin/env bash
set -euo pipefail

case "${1:-inspect}" in
inspect)
	rg -n -i 'frontier|progress|message|output|emit|batch|checkpoint' hat/hatReplication | head -n 360
	;;
benchmark)
	go test ./hat/hatReplication -run '^$' -bench 'BenchmarkSinkProgress' -benchmem -count=5
	;;
package)
	go test ./hat/hatReplication -count=1
	;;
race)
	go test -race ./hat/hatReplication -run 'TestSinkProgress' -count=1
	;;
vet)
	go vet ./hat/hatReplication
	;;
format)
	gofmt -w hat/hatReplication/m232_sink_progress.go hat/hatReplication/m232_sink_progress_test.go hat/hatReplication/m232_sink_progress_benchmark_test.go
	;;
test)
	go test ./hat/hatReplication -run 'TestSinkProgress' -count=1
	;;
esac
