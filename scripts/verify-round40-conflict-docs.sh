#!/usr/bin/env bash
set -euo pipefail

test -f TU38_CONFLICT_INTROSPECTION.md
rg -n 'TU38_CONFLICT_INTROSPECTION.md|T-U38|ConflictEventLog|Conflict introspection stream' README.md PRODUCT_IDEA_GAPS.md BENCHMARK.md TU38_CONFLICT_INTROSPECTION.md
