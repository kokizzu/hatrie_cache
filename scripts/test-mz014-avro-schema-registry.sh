#!/usr/bin/env bash
set -eu

mode="${1:-test}"
package='./hat/hatSql'
test_pattern='^TestMZ014AvroSchemaRegistry'
benchmark_pattern='^BenchmarkMZ014AvroSchemaRegistry'
paths=(
  Makefile
  README.md
  BENCHMARK.md
  INSPIRATION_BACKLOG.md
  MZ014_AVRO_SCHEMA_REGISTRY.md
  scripts/audit-next-ideas.sh
  scripts/test-mz014-avro-schema-registry.sh
  scripts/verify-mz014-avro-schema-registry-docs.sh
  hat/hatSql/mz014_avro_schema_registry.go
  hat/hatSql/mz014_avro_schema_registry_test.go
  hat/hatSql/mz014_avro_schema_registry_benchmark_test.go
)

case "$mode" in
  format)
    gofmt -w hat/hatSql/mz014_avro_schema_registry.go hat/hatSql/mz014_avro_schema_registry_test.go hat/hatSql/mz014_avro_schema_registry_benchmark_test.go
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
    git commit -m 'Add Avro schema registry cache [skip ci]'
    ;;
  push)
    git push origin HEAD:master
    ;;
  *)
    printf 'unknown mode: %s\n' "$mode" >&2
    exit 2
    ;;
esac
