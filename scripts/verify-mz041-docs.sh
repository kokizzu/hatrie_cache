#!/usr/bin/env bash
set -euo pipefail

rg -n -F -- 'MANAGED_REFRESH_FRESHNESS.md' README.md
rg -n -F -- 'MZ-041' ENGINE_IDEAS.md
rg -n -F -- 'View freshness SLA' ADOPTED_QUERY_ENGINE_IDEAS.md
rg -n -F -- '## Managed Refresh Freshness Status' BENCHMARK.md
rg -n -F -- '129.4 ns' MANAGED_REFRESH_FRESHNESS.md
rg -n -F -- '129.4 ns' BENCHMARK.md
