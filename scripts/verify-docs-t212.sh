#!/usr/bin/env bash
set -euo pipefail

for path in \
  T212_WAL_RETENTION_REPLICA_ACKS.md \
  BENCHMARK.md \
  INSPIRATION_ROUND2.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md; do
  test -f "$path"
done

rg -Fq 'T212_WAL_RETENTION_REPLICA_ACKS.md' INSPIRATION_ROUND2.md
rg -Fq 'T212_WAL_RETENTION_REPLICA_ACKS.md' ADOPTED_QUERY_ENGINE_IDEAS.md
rg -Fq 'T212 Replica-Acknowledgment WAL Retention' BENCHMARK.md
rg -Fq 'make benchmark-t212' BENCHMARK.md
rg -Fq 'ReplicaRetentionCapacity' T212_WAL_RETENTION_REPLICA_ACKS.md
