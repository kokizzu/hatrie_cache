#!/usr/bin/env bash
set -euo pipefail

git add Makefile README.md BENCHMARK.md INSPIRATION_ROUND2.md T242_APPEND_ONLY_AUDIT.md \
  hat/hatCache/monitoring.go \
  hat/hatCache/t242_audit_baseline_benchmark_test.go \
  hat/hatCache/t242_audit_test.go \
  scripts/t242-benchmark-baseline.sh scripts/t242-format.sh scripts/t242-test.sh \
  scripts/t242-test-package.sh scripts/t242-benchmark.sh scripts/t242-race.sh \
  scripts/t242-vet.sh scripts/stage-t242.sh scripts/commit-t242.sh scripts/push-t242.sh
git diff --cached --check
git diff --cached --stat
git status --short
