#!/usr/bin/env bash
set -euo pipefail
git add Makefile README.md INSPIRATION_ROUND2.md BENCHMARK.md T216_COMPACTION_SCHEDULING.md \
  hat/hatDataStructure/lsm_table.go \
  hat/hatDataStructure/lsm_compaction_scheduler.go \
  hat/hatDataStructure/t216_compaction_scheduler_test.go \
  hat/hatDataStructure/t216_compaction_baseline_benchmark_test.go \
  hat/hatDataStructure/t216_compaction_benchmark_test.go \
  scripts/test-t216.sh scripts/benchmark-t216-before.sh scripts/format-t216.sh \
  scripts/benchmark-t216.sh scripts/test-t216-package.sh scripts/race-t216.sh \
  scripts/vet-t216.sh scripts/report-t216-accounting.sh \
  scripts/verify-t216-scope.sh scripts/stage-t216.sh scripts/commit-t216.sh \
  scripts/push-t216.sh
