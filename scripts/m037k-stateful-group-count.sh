#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_root"

mode=${1:?usage: $0 baseline|format|test|benchmark|race|vet|package-test|check|status|commit|push}
cache_dir=$(mktemp -d /tmp/hatrie-cache-m037k-gocache.XXXXXX)
trap 'rm -rf "$cache_dir"' EXIT
export GOCACHE="$cache_dir"

case "$mode" in
  baseline)
    go test ./hat/hatSql -run '^$' -bench '^BenchmarkGroupCountDifferentialRows$' -benchmem -count=5
    ;;
  format)
    gofmt -w hat/hatSql/m037k_stateful_group_count.go hat/hatSql/m037k_stateful_group_count_test.go hat/hatSql/m037k_stateful_group_count_benchmark_test.go
    ;;
  test)
    go test ./hat/hatSql -run 'TestM037KStatefulGroupCount|ExampleIncrementalGroupCount' -count=1
    ;;
  benchmark)
    go test ./hat/hatSql -run '^$' -bench '^BenchmarkM037K' -benchmem -count=5 | tee M037K_BENCHMARK_RAW.txt
    ;;
  race)
    go test -race ./hat/hatSql -run 'TestM037KStatefulGroupCount|ExampleIncrementalGroupCount' -count=1
    ;;
  vet)
    go vet ./hat/hatSql
    ;;
  package-test)
    go test ./hat/hatSql -count=1
    ;;
  check)
    git diff --check
    ;;
  status)
    git status --short -- hat/hatSql/m037k_stateful_group_count.go hat/hatSql/m037k_stateful_group_count_test.go hat/hatSql/m037k_stateful_group_count_benchmark_test.go M037K_BENCHMARK_RAW.txt M037K_STATEFUL_GROUP_COUNT.md BENCHMARK.md INSPIRATION.md ADOPTED_QUERY_ENGINE_IDEAS.md CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md Makefile scripts/m037k-stateful-group-count.sh
    git diff -- Makefile
    ;;
  commit)
    git add hat/hatSql/m037k_stateful_group_count.go hat/hatSql/m037k_stateful_group_count_test.go hat/hatSql/m037k_stateful_group_count_benchmark_test.go M037K_BENCHMARK_RAW.txt M037K_STATEFUL_GROUP_COUNT.md BENCHMARK.md INSPIRATION.md ADOPTED_QUERY_ENGINE_IDEAS.md CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md Makefile scripts/m037k-stateful-group-count.sh
    git commit -m 'feat(hatSql): add stateful differential group count [skip ci]'
    ;;
  push)
    git push -u origin HEAD
    ;;
  *)
    printf 'unknown mode: %s\n' "$mode" >&2
    exit 2
    ;;
esac
