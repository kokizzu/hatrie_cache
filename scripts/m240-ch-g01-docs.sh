#!/usr/bin/env bash
set -euo pipefail
test -s CH001_ADAPTIVE_JOIN.md
rg -n 'CH001_ADAPTIVE_JOIN.md|CH-G01: Ordered Partial-Merge Join|SQLJoinAlgorithmPartialMerge' README.md BENCHMARK.md CH001_ADAPTIVE_JOIN.md IDEA_GAP_CATALOG.md
