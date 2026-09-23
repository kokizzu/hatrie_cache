#!/usr/bin/env bash
set -euo pipefail

rg -n '^## CH050 Plan Reproducibility Hash$|CH050_PLAN_REPRODUCIBILITY_HASH|SQLPlanReproducibilityHash|CH-G50' BENCHMARK.md README.md IDEA_GAP_CATALOG.md CH050_PLAN_REPRODUCIBILITY_HASH.md
