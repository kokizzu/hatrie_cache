#!/usr/bin/env bash
set -euo pipefail

test -f T206_REPLICA_BOOTSTRAP_JOIN.md
rg -q 'T206_REPLICA_BOOTSTRAP_JOIN.md' README.md
rg -q 'T206 Deterministic replica bootstrap and join workflow' INSPIRATION_ROUND2.md
rg -q 'ReplicaJoinAdmission' ADOPTED_QUERY_ENGINE_IDEAS.md
rg -q 't206-deterministic-replica-bootstrap-and-join' BENCHMARK.md
