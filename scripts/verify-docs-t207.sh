#!/usr/bin/env bash
set -euo pipefail

for file in \
  README.md \
  INSPIRATION_ROUND2.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  T207_REPLICA_EVICTION_RECOVERY.md; do
  test -s "$file"
done
rg -q 'T207_REPLICA_EVICTION_RECOVERY.md' README.md
rg -q 'T207 Replica eviction, rejoin, and stale-state recovery protocol' INSPIRATION_ROUND2.md
rg -q 'Replica eviction, rejoin, and stale-state recovery' ADOPTED_QUERY_ENGINE_IDEAS.md
rg -q 't207-replica-eviction-rejoin-and-stale-state-recovery' BENCHMARK.md
rg -q 'Existing ordinary join retry' T207_REPLICA_EVICTION_RECOVERY.md
