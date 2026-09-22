#!/usr/bin/env bash
set -euo pipefail

test -f T205_REPLICATION_PROGRESS_METRICS.md
rg -q 'T205_REPLICATION_PROGRESS_METRICS.md' README.md
rg -q 'T205 LSN-based replication lag and apply-throughput metrics' INSPIRATION_ROUND2.md
rg -q 'ReplicationProgressMetrics' ADOPTED_QUERY_ENGINE_IDEAS.md
rg -q 't205-replication-progress-metrics' BENCHMARK.md
