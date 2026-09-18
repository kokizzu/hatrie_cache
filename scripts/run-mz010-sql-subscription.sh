#!/usr/bin/env bash
set -eu

mode="${1:-test}"
case "$mode" in
  status)
    git status --short
    ;;
  format)
    gofmt -w hat/hatSql/sql_subscription.go hat/hatSql/mz010_sql_subscription_test.go hat/hatSql/mz010_sql_subscription_baseline_benchmark_test.go hat/hatSql/mz010_sql_subscription_benchmark_test.go
    ;;
  test)
    go test ./hat/hatSql -run 'TestMZ010' -count=1
    ;;
  benchmark)
    go test ./hat/hatSql -run '^$' -bench 'BenchmarkMZ010' -benchmem -count=5
    ;;
  baseline)
    go test -tags mz010baseline ./hat/hatSql -run '^$' -bench 'BenchmarkMZ010ManualSubscription' -benchmem -count=5
    ;;
  race)
    go test -race ./hat/hatSql -run 'TestMZ010' -count=1
    ;;
  vet)
    go vet ./hat/hatSql
    ;;
  package)
    go test ./hat/hatSql -count=1
    ;;
  stage)
    git add Makefile README.md BENCHMARK.md ENGINE_IDEAS.md ADOPTED_QUERY_ENGINE_IDEAS.md MZ010_JOURNAL_SUBSCRIPTIONS.md MZ010_SQL_SUBSCRIPTIONS.md hat/hatSql/sql_subscription.go hat/hatSql/mz010_sql_subscription_test.go hat/hatSql/mz010_sql_subscription_baseline_benchmark_test.go hat/hatSql/mz010_sql_subscription_benchmark_test.go scripts/run-mz010-sql-subscription.sh
    ;;
  commit)
    git commit -m 'add SQL subscription dependency discovery [skip ci]'
    ;;
  push)
    git push origin HEAD:master
    ;;
  *)
    printf 'unknown MZ-010 SQL subscription mode: %s\n' "$mode" >&2
    exit 2
    ;;
esac
