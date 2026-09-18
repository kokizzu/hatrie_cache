#!/usr/bin/env bash
set -eu

mode="${1:-test}"
package='./hat/hatSql'
test_pattern='^TestKafkaTableSource'
benchmark_pattern='^BenchmarkKafkaTableSource'
paths=(
  Makefile
  README.md
  BENCHMARK.md
  INSPIRATION_BACKLOG.md
  CH046_KAFKA_TABLE_SOURCE.md
  scripts/test-ch046-kafka-table-source.sh
  scripts/verify-ch046-kafka-table-source-docs.sh
  hat/hatSql/ch046_kafka_table_source.go
  hat/hatSql/ch046_kafka_table_source_test.go
  hat/hatSql/ch046_kafka_table_source_benchmark_test.go
)

case "$mode" in
  format)
    gofmt -w hat/hatSql/ch046_kafka_table_source.go hat/hatSql/ch046_kafka_table_source_test.go hat/hatSql/ch046_kafka_table_source_benchmark_test.go
    ;;
  test)
    go test "$package" -run "$test_pattern" -count=1
    ;;
  benchmark)
    go test "$package" -run '^$' -bench "$benchmark_pattern" -benchmem -count=5
    ;;
  race)
    go test -race "$package" -run "$test_pattern" -count=1
    ;;
  vet)
    go vet "$package"
    ;;
  package)
    go test "$package" -count=1
    ;;
  stage)
    git add "${paths[@]}"
    git diff --cached --check
    git status --short
    ;;
  commit)
    git add "${paths[@]}"
    git diff --cached --check
    git commit -m 'Add Kafka table source checkpoints [skip ci]'
    ;;
  push)
    git push origin HEAD:master
    ;;
  *)
    printf 'unknown mode: %s\n' "$mode" >&2
    exit 2
    ;;
esac
