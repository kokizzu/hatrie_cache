#!/usr/bin/env bash
set -euo pipefail

git add -- \
  Makefile PRODUCT_IDEA_GAPS.md ADOPTED_QUERY_ENGINE_IDEAS.md INSPIRATION.md \
  BENCHMARK.md TU19_TUPLE_UPDATE_JOURNAL.md \
  hat/hatDataStructure/tuple_field_update_journal.go \
  hat/hatDataStructure/t_u19_tuple_update_journal_test.go \
  hat/hatDataStructure/t_u19_tuple_update_journal_baseline_benchmark_test.go \
  hat/hatDataStructure/t_u19_tuple_update_journal_benchmark_test.go \
  scripts/benchmark-t-u19-baseline.sh scripts/benchmark-t-u19-replay.sh \
  scripts/benchmark-t-u19.sh scripts/commit-t-u19.sh scripts/format-t-u19.sh \
  scripts/push-t-u19.sh scripts/race-t-u19.sh scripts/stage-t-u19.sh \
  scripts/test-t-u19-package.sh scripts/test-t-u19.sh scripts/verify-t-u19.sh \
  scripts/vet-t-u19.sh
git diff --cached --name-only
