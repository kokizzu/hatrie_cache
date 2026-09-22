#!/usr/bin/env bash
set -euo pipefail

for file in \
  M247_FRONTIER_EXPIRY_ERRORS.md \
  README.md \
  BENCHMARK.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  INSPIRATION_ROUND2.md; do
  test -f "$file"
done

rg -q 'M247_FRONTIER_EXPIRY_ERRORS.md' README.md
rg -q 'M247 Frontier Expiry Errors' BENCHMARK.md
rg -q 'M247 Resume errors' INSPIRATION_ROUND2.md
rg -q 'FrontierRetentionExpiredError' ADOPTED_QUERY_ENGINE_IDEAS.md
printf '%s\n' 'M247 documentation verified'
