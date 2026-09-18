#!/usr/bin/env bash
set -euo pipefail

mode="${1:-test}"
package="./hat/hatSql"
test_pattern='^TestMZ013DebeziumKafka'
benchmark_pattern='^BenchmarkMZ013DebeziumKafka'

case "$mode" in
  format)
    gofmt -w \
      hat/hatSql/mz013_debezium_kafka_decoder.go \
      hat/hatSql/mz013_debezium_kafka_decoder_test.go \
      hat/hatSql/mz013_debezium_kafka_decoder_benchmark_test.go
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
      MZ013_DEBEZIUM_KAFKA_DECODER.md \
      Makefile \
      README.md \
      hat/hatSql/mz013_debezium_kafka_decoder.go \
      hat/hatSql/mz013_debezium_kafka_decoder_test.go \
      hat/hatSql/mz013_debezium_kafka_decoder_benchmark_test.go \
      scripts/test-mz013-debezium-kafka.sh \
      scripts/verify-mz013-debezium-kafka-docs.sh
    git status --short
    ;;
  commit)
    git commit -m 'Add Debezium Kafka envelope normalization [skip ci]'
    ;;
  push)
    git push origin HEAD:master
    ;;
  *)
    printf 'usage: %s {format|test|benchmark|race|vet|package|stage|commit|push}\n' "$0" >&2
    exit 2
    ;;
esac
