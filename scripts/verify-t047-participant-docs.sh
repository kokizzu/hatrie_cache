#!/usr/bin/env bash
set -euo pipefail

rg -q '^# T047 Durable Participant State$' T047_PARTICIPANT_STATE.md
rg -q 'ClusterWriteCommitParticipant' T047_PARTICIPANT_STATE.md
rg -q '^## T047 Durable Participant State$' BENCHMARK.md
rg -q 'T047f Bounded durable participant phase state' CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md
rg -q 'T047_PARTICIPANT_STATE.md' T047_CLUSTER_WRITE_COMMIT.md
git diff --check -- BENCHMARK.md CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md T047_CLUSTER_WRITE_COMMIT.md T047_PARTICIPANT_STATE.md
