#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "$0")/.." && pwd)
cd "$repo_root"
mode=${1:-verify}

case "$mode" in
  format)
    gofmt -w hat/hatCache/monitoring.go hat/hatCache/monitoring_optimize_test.go
    ;;
  test)
    go test ./hat/hatCache -run 'TestMonitoring.*Optimize' -count=1
    ;;
  race)
    go test -race ./hat/hatCache -run 'TestMonitoring.*Optimize' -count=1
    ;;
  vet)
    go vet ./hat/hatCache
    ;;
  readme)
    go test ./hat/hatCache -run 'TestREADME' -count=1
    ;;
  benchmark)
    cache=/tmp/hatrie-chu35-optimize-gocache-20261007
    rm -rf "$cache"
    mkdir -p "$cache"
    trap 'rm -rf "$cache"' EXIT
    GOCACHE="$cache" go test ./hat/hatCache ./hat/hatStorage -run '^$' -bench 'BenchmarkMonitoringOptimize|BenchmarkCompactionSchedulerRun$|BenchmarkCHU35CompactionControllerRun' -benchmem -count=5
    ;;
  verify)
    git diff --check -- \
      BENCHMARK.md \
      CHU35_OPTIMIZE_CONTROL.md \
      Makefile \
      PRODUCT_IDEA_GAPS.md \
      README.md \
      hat/hatCache/monitoring.go \
      hat/hatCache/monitoring_optimize_test.go \
      scripts/run-chu35-optimize-adapter.sh
    go test ./hat/hatCache -run 'TestMonitoring.*Optimize' -count=1
    go test -race ./hat/hatCache -run 'TestMonitoring.*Optimize' -count=1
    go vet ./hat/hatCache
    ;;
  *)
    printf 'unknown mode: %s\n' "$mode" >&2
    exit 2
    ;;
esac
