#!/usr/bin/env bash
set -euo pipefail
git add Makefile README.md INSPIRATION_ROUND2.md BENCHMARK.md T215_PER_SPACE_STORAGE_POLICY.md \
  hat/hatDataStructure/space.go \
  hat/hatDataStructure/t215_space_policy_test.go \
  hat/hatDataStructure/t215_space_policy_baseline_benchmark_test.go \
  hat/hatDataStructure/t215_space_policy_benchmark_test.go \
  scripts/test-t215.sh scripts/benchmark-t215-before.sh scripts/format-t215.sh \
  scripts/benchmark-t215.sh scripts/test-t215-package.sh scripts/race-t215.sh \
  scripts/vet-t215.sh scripts/verify-t215-scope.sh scripts/stage-t215.sh \
  scripts/commit-t215.sh scripts/push-t215.sh
