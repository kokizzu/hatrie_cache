#!/usr/bin/env bash
set -euo pipefail

case "${1:-inspect}" in
inspect)
	rg -n -i 'retry|backoff|dedup|outbox|connection|queue|disconnect|reconnect' hat/hatReplication | head -n 360
	;;
format)
	gofmt -w hat/hatReplication/m233_sink_retry.go hat/hatReplication/m233_sink_retry_test.go hat/hatReplication/m233_sink_retry_benchmark_test.go
	;;
test)
	go test ./hat/hatReplication -run 'TestSinkRetry' -count=1
	;;
esac
