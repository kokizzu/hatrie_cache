#!/usr/bin/env bash
set -euo pipefail

git add -- \
  BENCHMARK.md \
  ENGINE_IDEAS.md \
  MZ004_COMPACTION_POLICY.md \
  Makefile \
  hat/hatPipeline/frontier_compaction_policy.go \
  hat/hatPipeline/frontier_compaction_scheduler.go \
  hat/hatPipeline/mz004_compaction_policy_benchmark_test.go \
  hat/hatPipeline/mz004_compaction_policy_test.go \
  scripts/benchmark-mz004-compaction-policy.sh \
  scripts/format-mz004-compaction-policy.sh \
  scripts/race-mz004-compaction-policy.sh \
  scripts/stage-mz004-compaction-policy.sh \
  scripts/test-mz004-repository.sh \
  scripts/test-mz004-compaction-policy-package.sh \
  scripts/test-mz004-compaction-policy.sh \
  scripts/vet-mz004-compaction-policy.sh
