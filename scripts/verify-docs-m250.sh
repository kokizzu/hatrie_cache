#!/usr/bin/env bash
set -euo pipefail

test -f M250_TEMPORAL_JOIN_FRONTIER_ALIGNMENT.md
rg -q 'M250_TEMPORAL_JOIN_FRONTIER_ALIGNMENT.md' README.md
rg -q 'Temporal join frontier alignment' ADOPTED_QUERY_ENGINE_IDEAS.md
rg -q '\[x\] M250' INSPIRATION_ROUND2.md
rg -q 'm250-temporal-join-frontier-alignment' BENCHMARK.md
rg -q '5.282' BENCHMARK.md
