#!/usr/bin/env bash
set -euo pipefail

test -f T201_PER_SPACE_SYNC_QUORUM.md
rg -q 'T201_PER_SPACE_SYNC_QUORUM.md' README.md
rg -q 'Per-space synchronous replication quorum' ADOPTED_QUERY_ENGINE_IDEAS.md
rg -q '\[x\] T201' INSPIRATION_ROUND2.md
rg -q 't201-per-space-synchronous-replication-quorum' BENCHMARK.md
rg -q '1,109' BENCHMARK.md
