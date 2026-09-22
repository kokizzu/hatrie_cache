#!/usr/bin/env bash
set -euo pipefail

case "${1:-}" in
  inspect)
    sed -n '1,320p' hat/hatReplication/m233_sink_retry.go
    rg -n -C 4 'Backpressure|SinkProgress|RetryQueue|MaxPending|QueueFull' hat/hatReplication
    ;;
  format)
    gofmt -w \
      hat/hatReplication/m234_sink_backpressure.go \
      hat/hatReplication/m234_sink_backpressure_test.go \
      hat/hatReplication/m234_sink_backpressure_benchmark_test.go
    ;;
  test)
    go test ./hat/hatReplication -run 'TestSinkRetryBackpressure' -count=1
    ;;
  benchmark)
    go test ./hat/hatReplication -run '^$' -bench 'BenchmarkSinkRetryBackpressure' -benchmem -count=5
    ;;
  package)
    go test ./hat/hatReplication -count=1
    ;;
  race)
    go test -race ./hat/hatReplication -count=1
    ;;
  vet)
    go vet ./hat/hatReplication
    ;;
  docs)
    test -s M234_SINK_BACKPRESSURE.md
    rg -n 'M234_SINK_BACKPRESSURE.md|## M234 Sink Backpressure' INSPIRATION_ROUND2.md ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md M234_SINK_BACKPRESSURE.md
    ;;
  *)
    echo "usage: $0 inspect|format|test|benchmark|package|race|vet|docs" >&2
    exit 2
    ;;
esac
