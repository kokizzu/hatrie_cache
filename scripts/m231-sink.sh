#!/usr/bin/env bash
set -euo pipefail

case "${1:-inspect}" in
inspect)
	rg -n -i 'sink|upsert|dedup|idempot|output|checkpoint|commit' hat/hatReplication | head -n 320
	;;
m228)
	sed -n '1,340p' hat/hatReplication/m228_exactly_once_restart.go
	sed -n '340,760p' hat/hatReplication/m228_exactly_once_restart.go
	;;
benchmark)
	go test ./hat/hatReplication -run '^$' -bench 'BenchmarkExactlyOnceUpsertSink' -benchmem -count=5
	;;
package)
	go test ./hat/hatReplication -count=1
	;;
race)
	go test -race ./hat/hatReplication -run 'TestExactlyOnceUpsertSink' -count=1
	;;
vet)
	go vet ./hat/hatReplication
	;;
format)
	gofmt -w hat/hatReplication/m231_exactly_once_upsert_sink.go hat/hatReplication/m231_exactly_once_upsert_sink_test.go hat/hatReplication/m231_exactly_once_upsert_sink_benchmark_test.go
	;;
test)
	go test ./hat/hatReplication -run 'TestExactlyOnceUpsertSink' -count=1
	;;
esac
