#!/usr/bin/env bash
set -euo pipefail

rg -n 'CH-U10|Feedback-Driven Projection Selection|CostRecommendations|4\.34-4\.55|7\.11-7\.24|17\.52-18\.96' \
  README.md PRODUCT_IDEA_GAPS.md ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md \
  CHU10_FEEDBACK_PROJECTION_SELECTION.md
