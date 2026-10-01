#!/usr/bin/env bash
set -euo pipefail

git add -- \
  Makefile PRODUCT_IDEA_GAPS.md ADOPTED_QUERY_ENGINE_IDEAS.md INSPIRATION.md \
  BENCHMARK.md TU038_CONFLICT_INTROSPECTION.md \
  hat/hatReplication/conflict_policy.go \
  hat/hatReplication/tu38_conflict_introspection.go \
  hat/hatReplication/tu38_conflict_introspection_test.go \
  hat/hatReplication/tu38_conflict_introspection_benchmark_test.go \
  scripts/benchmark-t-u38-baseline.sh scripts/benchmark-t-u38.sh \
  scripts/commit-t-u38.sh scripts/format-t-u38.sh scripts/push-t-u38.sh \
  scripts/race-t-u38.sh scripts/stage-t-u38.sh scripts/test-t-u38-baseline.sh \
  scripts/verify-t-u38.sh scripts/vet-t-u38.sh
git diff --cached --name-only
