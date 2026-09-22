#!/usr/bin/env bash
set -euo pipefail

test -f M242_OPERATOR_METRICS.md
rg -q 'OperatorMetrics|UpdateCount|BatchCount|Frontier' M242_OPERATOR_METRICS.md
rg -q 'M242_OPERATOR_METRICS.md' README.md INSPIRATION_ROUND2.md ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md
rg -q 'M242 Per-Operator Update, Batch, And Frontier Metrics' BENCHMARK.md
