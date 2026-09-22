#!/usr/bin/env bash
set -euo pipefail

mode="${1:-inspect}"

case "$mode" in
  inspect)
    sed -n '280,620p' hat/hatReplication/tu39_space_changefeed.go
    ;;
  format)
    gofmt -w hat/hatReplication/m229_source_schema.go hat/hatReplication/m229_source_schema_test.go hat/hatReplication/m229_source_schema_benchmark_test.go
    ;;
  test)
    go test ./hat/hatReplication -run 'TestChangefeedSchema|TestSpaceChangefeedTypedSchema' -count=1
    ;;
  benchmark)
    go test ./hat/hatReplication -run '^$' -bench 'BenchmarkChangefeedSchema|BenchmarkSpaceChangefeedPublish' -benchmem -count=5
    ;;
  race)
    go test -race ./hat/hatReplication -run 'TestChangefeedSchema|TestSpaceChangefeedTypedSchema' -count=1
    ;;
  vet)
    go vet ./hat/hatReplication
    ;;
  package)
    go test ./hat/hatReplication -count=1
    ;;
  docs)
    test -s M229_SOURCE_SCHEMA_EVOLUTION.md
    grep -Fq 'M229 Source Schema Evolution' BENCHMARK.md
    grep -Fq 'M229 Source Schema Evolution' ADOPTED_QUERY_ENGINE_IDEAS.md
    grep -Fq 'M229' INSPIRATION_ROUND2.md
    ;;
  *)
    printf 'unknown M229 mode: %s\n' "$mode" >&2
    exit 2
    ;;
esac
