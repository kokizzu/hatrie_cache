#!/usr/bin/env bash
set -euo pipefail

mode="${1:-test}"
case "$mode" in
  format)
    gofmt -w hat/hatReplication/m226_consensus_metadata.go hat/hatReplication/m226_consensus_metadata_test.go
    ;;
  test)
    go test ./hat/hatReplication -run 'TestShardConsensusMetadata' -count=1
    ;;
  benchmark)
    go test ./hat/hatReplication -run '^$' -bench 'BenchmarkShardConsensusMetadata' -benchmem -count=5
    ;;
  size)
    go test ./hat/hatReplication -run '^TestShardConsensusMetadataBinaryIsSmallerThanJSON$' -v -count=1
    ;;
  race)
    go test -race ./hat/hatReplication -run 'TestShardConsensusMetadata' -count=1
    ;;
  vet)
    go vet ./hat/hatReplication
    ;;
  package)
    go test ./hat/hatReplication -count=1
    ;;
  docs)
    rg -q 'M226_DURABLE_CONSENSUS_METADATA.md' INSPIRATION_ROUND2.md ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md
    rg -q '## M226' M226_DURABLE_CONSENSUS_METADATA.md ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md
    ;;
  *)
    printf 'unknown M226 mode: %s\n' "$mode" >&2
    exit 2
    ;;
esac
