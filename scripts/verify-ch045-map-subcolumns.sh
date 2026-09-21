#!/usr/bin/env bash
set -euo pipefail

test -f CH045_NESTED_MAP_SUBCOLUMNS.md
rg -q 'CH-045 Nested Map Subcolumn Pruning' CH045_NESTED_MAP_SUBCOLUMNS.md
rg -q 'CH-045.*Partially adopted' ENGINE_IDEAS.md
rg -q 'CH-045 Nested Map Subcolumn Pruning' BENCHMARK.md
rg -q '28\.6x' BENCHMARK.md
rg -q '16\.4x lower' BENCHMARK.md
printf '%s\n' 'CH-045 documentation verified'
