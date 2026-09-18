#!/usr/bin/env bash
set -euo pipefail

mode="${1:-test}"
package="./hat/hatSql"
test_pattern='^TestMZ016KafkaSourceCheckpoint'
benchmark_pattern='^BenchmarkMZ016KafkaSource'

case "$mode" in
  format)
    gofmt -w \
      hat/hatSql/mz016_kafka_source_checkpoint.go \
      hat/hatSql/mz016_kafka_source_checkpoint_test.go \
      hat/hatSql/mz016_kafka_source_checkpoint_benchmark_test.go
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
    git add \
      BENCHMARK.md \
      INSPIRATION_BACKLOG.md \
      MZ016_KAFKA_SOURCE_CHECKPOINT.md \
      Makefile \
      README.md \
      hat/hatSql/ch046_kafka_table_source.go \
      hat/hatSql/mz016_kafka_source_checkpoint.go \
      hat/hatSql/mz016_kafka_source_checkpoint_test.go \
      hat/hatSql/mz016_kafka_source_checkpoint_benchmark_test.go \
      scripts/test-mz016-kafka-source-checkpoint.sh \
      scripts/verify-mz016-kafka-source-checkpoint-docs.sh
    git status --short
    ;;
  commit)
    git commit -m 'Add durable Kafka source checkpoints [skip ci]'
    ;;
  push)
    git push origin HEAD:master
    ;;
  *)
    printf 'usage: %s {format|test|benchmark|race|vet|package|stage|commit|push}\n' "$0" >&2
    exit 2
    ;;
esac
