#!/usr/bin/env bash
set -euo pipefail

test -f C239_COMPACTION_METRICS.md
rg -q 'C239_COMPACTION_METRICS.md' INSPIRATION_ROUND2.md
rg -q 'C239: Part-Merge Backlog And Amplification Metrics' ADOPTED_QUERY_ENGINE_IDEAS.md
rg -q 'C239: Compaction Metrics' BENCHMARK.md
rg -q 'make benchmark-c239' C239_COMPACTION_METRICS.md
