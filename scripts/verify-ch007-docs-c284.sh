#!/usr/bin/env bash
set -euo pipefail
for file in CH007_ROW_TTL.md CH007_TTL_SCHEDULER.md README.md ENGINE_IDEAS.md ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md; do
  test -f "$file"
done
rg -q 'CH007_TTL_SCHEDULER.md' README.md
rg -q 'CH-007.*Adopted' ENGINE_IDEAS.md
rg -q 'TypedTableTTLScheduler' ADOPTED_QUERY_ENGINE_IDEAS.md
rg -q 'CH-007 Background TTL Scheduler' BENCHMARK.md
