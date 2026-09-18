#!/usr/bin/env bash
set -euo pipefail

mode=${1:-}
package=./hat/hatReplication
test_pattern='^TestApplierThrottle'
benchmark_pattern='^BenchmarkTU37'

case "$mode" in
  format)
    gofmt -w hat/hatReplication/applier_throttle.go \
      hat/hatReplication/tu37_applier_throttle_test.go \
      hat/hatReplication/tu37_applier_throttle_baseline_benchmark_test.go \
      hat/hatReplication/tu37_applier_throttle_benchmark_test.go \
      hat/hatCache/tu37_replica_applier_throttle.go \
      hat/hatCache/tu37_replica_applier_throttle_test.go \
      hat/hatCache/grpc.go \
      hat/hatCache/journal_pull.go
    ;;
  test)
    go test "$package" -run "$test_pattern" -count=1
    ;;
  cache-test)
    go test ./hat/hatCache -run '^TestTU37' -count=1
    ;;
  baseline)
    go test -tags tu37baseline "$package" -run '^$' -bench '^BenchmarkTU37UnthrottledApplyAdmission$' -benchmem -count=5
    ;;
  benchmark)
    go test "$package" -run '^$' -bench "$benchmark_pattern" -benchmem -count=5
    ;;
  race)
    go test -race "$package" -run "$test_pattern" -count=1
    ;;
  cache-race)
    go test -race ./hat/hatCache -run '^TestTU37' -count=1
    ;;
  vet)
    go vet "$package"
    ;;
  cache-vet)
    go vet ./hat/hatCache
    ;;
  package)
    go test "$package" -count=1
    ;;
  cache-package)
    go test ./hat/hatCache -count=1
    ;;
  stage)
    git add Makefile README.md ADOPTED_QUERY_ENGINE_IDEAS.md PRODUCT_IDEA_GAPS.md TU37_REPLICA_APPLIER_THROTTLE.md \
      BENCHMARK.md hat/hatCache/grpc.go hat/hatCache/journal_pull.go \
      hat/hatCache/tu37_replica_applier_throttle.go \
      hat/hatCache/tu37_replica_applier_throttle_test.go \
      hat/hatReplication/applier_throttle.go \
      hat/hatReplication/tu37_applier_throttle_test.go \
      hat/hatReplication/tu37_applier_throttle_baseline_benchmark_test.go \
      hat/hatReplication/tu37_applier_throttle_benchmark_test.go \
      scripts/run-tu37-applier-throttle.sh
    ;;
  commit)
    git commit -m 'add opt-in replica applier throttling [skip ci]'
    ;;
  push)
    git push origin HEAD:master
    ;;
  status)
    git status --short
    git diff --stat
    git diff --cached --stat
    ;;
  *)
    printf 'usage: %s {format|test|cache-test|baseline|benchmark|race|cache-race|vet|cache-vet|package|cache-package|stage|commit|push|status}\n' "$0" >&2
    exit 2
    ;;
esac
