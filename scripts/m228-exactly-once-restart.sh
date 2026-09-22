#!/usr/bin/env bash
set -euo pipefail

mode="${1:-test}"
case "$mode" in
  format)
    gofmt -w hat/hatReplication/m228_exactly_once_restart.go hat/hatReplication/m228_exactly_once_restart_test.go hat/hatReplication/m228_exactly_once_restart_benchmark_test.go
    ;;
  test)
    go test ./hat/hatReplication -run 'TestChangefeedExactlyOnce' -count=1
    ;;
  benchmark)
    go test ./hat/hatReplication -run '^$' -bench 'BenchmarkChangefeedExactlyOnce' -benchmem -count=5
    ;;
  size)
    go test ./hat/hatReplication -run '^TestChangefeedExactlyOnceBinaryIsSmallerThanJSON$' -v -count=1
    ;;
  race)
    go test -race ./hat/hatReplication -run 'TestChangefeedExactlyOnce' -count=1
    ;;
  vet)
    go vet ./hat/hatReplication
    ;;
  package)
    go test ./hat/hatReplication -count=1
    ;;
  docs)
    test -s M228_EXACTLY_ONCE_SOURCE_RESTART.md
    grep -Fq 'M228 Exactly-Once Source Restart' BENCHMARK.md
    grep -Fq 'M228 Exactly-Once Source Restart' ADOPTED_QUERY_ENGINE_IDEAS.md
    grep -Fq 'M228' INSPIRATION_ROUND2.md
    ;;
  *)
    printf 'unknown M228 mode: %s\n' "$mode" >&2
    exit 2
    ;;
esac
