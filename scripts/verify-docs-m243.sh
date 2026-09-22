#!/usr/bin/env bash
set -euo pipefail

test -f M243_ARRANGEMENT_MEMORY_METRICS.md
rg -q 'EstimatedKeyBytes|EstimatedValueBytes|EstimatedTraceBytes' M243_ARRANGEMENT_MEMORY_METRICS.md
rg -q 'M243_ARRANGEMENT_MEMORY_METRICS.md' README.md INSPIRATION_ROUND2.md ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md
rg -q 'M243 Arrangement Memory Metrics' BENCHMARK.md
