#!/usr/bin/env bash
set -euo pipefail

test -f CHU18_COMPOSITE_PRIMARY_MARK_PRUNING.md
rg -q 'CHU18_COMPOSITE_PRIMARY_MARK_PRUNING.md' README.md
rg -q 'CH-U18' PRODUCT_IDEA_GAPS.md
rg -q 'Composite primary marks' ADOPTED_QUERY_ENGINE_IDEAS.md
rg -q 'ch-u18-composite-primary-mark-pruning' BENCHMARK.md
rg -q 'SparsePrimaryFields' CHU18_COMPOSITE_PRIMARY_MARK_PRUNING.md
printf '%s\n' 'CH-U18 documentation verification passed.'
