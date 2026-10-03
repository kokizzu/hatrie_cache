#!/usr/bin/env bash
set -euo pipefail

mode=${1:-unit}
case "$mode" in
  unit)
    go test ./hat/hatFiber -run '^TestScheduler'
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
    cache=$(mktemp -d /tmp/hatrie-t030-gocache.XXXXXX)
    trap 'rm -rf "$cache"' EXIT
    GOCACHE="$cache" go test ./...
    ;;
  format)
    gofmt -w hat/hatFiber/fiber.go hat/hatFiber/fiber_test.go hat/hatFiber/fiber_benchmark_test.go
    ;;
  benchmark)
    go test ./hat/hatFiber -run '^$' -bench 'Benchmark(ContinuationFibers|GoroutinePerFlow)' -benchmem -count=5
    ;;
  benchmark-feature-raw)
    {
      printf '%s\n' 'T030 feature benchmark: continuation scheduler, lifecycle and steady-state'
      printf '%s\n' 'command: go test ./hat/hatFiber -run ^$ -bench BenchmarkContinuationFibers -benchmem -count=5'
      go test ./hat/hatFiber -run '^$' -bench 'BenchmarkContinuationFibers' -benchmem -count=5
    } | tee T030_BENCHMARK_RAW.txt
    ;;
  benchmark-reference-raw)
    {
      printf '%s\n' 'T030 reference benchmark: goroutine-per-flow equivalent workload'
      printf '%s\n' 'command: go test ./hat/hatFiber -run ^$ -bench ^BenchmarkGoroutinePerFlow$ -benchmem -count=5'
      go test ./hat/hatFiber -run '^$' -bench '^BenchmarkGoroutinePerFlow$' -benchmem -count=5
    } | tee T030_BENCHMARK_BASELINE_RAW.txt
    ;;
  *)
    printf '%s\n' "unknown T030 verification mode: $mode" >&2
    exit 2
    ;;
esac
