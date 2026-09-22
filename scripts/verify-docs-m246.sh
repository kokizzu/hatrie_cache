#!/usr/bin/env bash
set -euo pipefail

for file in \
  M246_FRONTIER_RETENTION_POLICY.md \
  README.md \
  BENCHMARK.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  INSPIRATION_ROUND2.md; do
  test -f "$file"
done

rg -q 'M246_FRONTIER_RETENTION_POLICY.md' README.md
rg -q 'M246 Per-Object History-Retention Policies' BENCHMARK.md
rg -q 'M246 Per-object history-retention policies' INSPIRATION_ROUND2.md
rg -q 'FrontierRetentionRegistry.SetPolicy' ADOPTED_QUERY_ENGINE_IDEAS.md
printf '%s\n' 'M246 documentation verified'
