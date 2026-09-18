#!/usr/bin/env bash
set -eu

mode="${1:-test}"
package='./hat/hatSql'
test_pattern='^TestMZ015ProtobufSchemaRegistry'
benchmark_pattern='^BenchmarkMZ015ProtobufSchemaRegistry'
paths=(
  Makefile
  README.md
  BENCHMARK.md
  INSPIRATION_BACKLOG.md
  MZ015_PROTOBUF_SCHEMA_REGISTRY.md
  scripts/test-mz015-protobuf-schema-registry.sh
  scripts/verify-mz015-protobuf-schema-registry-docs.sh
  hat/hatSql/mz015_protobuf_schema_registry.go
  hat/hatSql/mz015_protobuf_schema_registry_test.go
  hat/hatSql/mz015_protobuf_schema_registry_benchmark_test.go
)

case "$mode" in
  format)
    gofmt -w hat/hatSql/mz015_protobuf_schema_registry.go hat/hatSql/mz015_protobuf_schema_registry_test.go hat/hatSql/mz015_protobuf_schema_registry_benchmark_test.go
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
    git commit -m 'Add Protobuf schema registry integration [skip ci]'
    ;;
  push)
    git push origin HEAD:master
    ;;
  *)
    printf 'unknown mode: %s\n' "$mode" >&2
    exit 2
    ;;
esac
