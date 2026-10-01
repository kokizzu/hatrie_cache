#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' 'checking procedure documentation file'
test -s TU03_STORED_PROCEDURE_REGISTRY.md
printf '%s\n' 'checking README link'
rg -n 'TU03_STORED_PROCEDURE_REGISTRY.md' README.md
printf '%s\n' 'checking benchmark result'
rg -n '249.2x' BENCHMARK.md
printf '%s\n' 'checking catalog marker'
rg -n 'hatProcedure.Registry' PRODUCT_IDEA_GAPS.md
