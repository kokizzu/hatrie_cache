#!/usr/bin/env bash
set -euo pipefail

mode=${1:-unit}
case "$mode" in
  unit)
    go test -timeout 5s -count=10 ./hat/hatFiber -run '^TestT031'
    ;;
  package)
    go test ./hat/hatFiber
    ;;
  race)
    go test -race ./hat/hatFiber
    ;;
  vet)
    go vet ./hat/hatFiber
    ;;
  full)
    cache=$(mktemp -d /tmp/hatrie-t031-gocache.XXXXXX)
    trap 'rm -rf "$cache"' EXIT
    GOCACHE="$cache" go test ./...
    ;;
  format)
    gofmt -w hat/hatFiber/coordination.go hat/hatFiber/coordination_test.go hat/hatFiber/coordination_benchmark_test.go
    ;;
  benchmark)
    go test ./hat/hatFiber -run '^$' -bench 'BenchmarkT031' -benchmem -count=5
    ;;
  benchmark-feature-raw)
    {
      printf '%s\n' 'T031 feature benchmark: fiber-aware channel round trip'
      printf '%s\n' 'command: go test ./hat/hatFiber -run ^$ -bench ^BenchmarkT031FiberChannelRoundTrip$ -benchmem -count=5'
      go test ./hat/hatFiber -run '^$' -bench '^BenchmarkT031FiberChannelRoundTrip$' -benchmem -count=5 | sed 's/[[:space:]]*$//'
    } | tee T031_BENCHMARK_RAW.txt
    ;;
  benchmark-reference-raw)
    {
      printf '%s\n' 'T031 reference benchmark: existing hatPipeline channel round trip'
      printf '%s\n' 'command: go test ./hat/hatFiber -run ^$ -bench ^BenchmarkT031PipelineChannelRoundTrip$ -benchmem -count=5'
      go test ./hat/hatFiber -run '^$' -bench '^BenchmarkT031PipelineChannelRoundTrip$' -benchmem -count=5 | sed 's/[[:space:]]*$//'
    } | tee T031_BENCHMARK_BASELINE_RAW.txt
    ;;
  *)
    printf '%s\n' "unknown T031 verification mode: $mode" >&2
    exit 2
    ;;
esac
