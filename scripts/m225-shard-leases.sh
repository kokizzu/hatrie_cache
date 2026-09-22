#!/usr/bin/env bash
set -euo pipefail

mode="${1:-test}"
case "$mode" in
  format)
    gofmt -w hat/hatReplication/m225_shard_lease.go hat/hatReplication/m225_shard_lease_test.go hat/hatReplication/m225_shard_lease_benchmark_test.go
    ;;
  test)
    go test ./hat/hatReplication -run 'TestShardLease' -count=1
    ;;
  size)
    go test ./hat/hatReplication -run '^TestShardLeaseSnapshotBinaryIsSmallerThanJSON$' -v -count=1
    ;;
  benchmark)
    go test ./hat/hatReplication -run '^$' -bench 'BenchmarkShardLease' -benchmem -count=5
    ;;
  race)
    go test -race ./hat/hatReplication -run 'TestShardLeaseRegistry' -count=1
    ;;
  vet)
    go vet ./hat/hatReplication
    ;;
  package)
    go test ./hat/hatReplication -count=1
    ;;
  docs)
    rg -q 'M225_PERSISTED_SHARD_LEASES.md' INSPIRATION_ROUND2.md ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md
    rg -q '## M225' M225_PERSISTED_SHARD_LEASES.md ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md
    ;;
  *)
    printf 'unknown M225 mode: %s\n' "$mode" >&2
    exit 2
    ;;
esac
