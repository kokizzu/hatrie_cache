#!/usr/bin/env bash
set -euo pipefail

git add Makefile INSPIRATION.md README.md SCHEMA_ROLLOUT.md \
  hat/hatSchema/rolling_schema.go \
  hat/hatSchema/rolling_schema_coordinator_test.go \
  hat/hatSchema/rolling_schema_orchestration_benchmark_test.go \
  scripts/benchmark-c154d.sh scripts/format-c154d.sh scripts/race-c154d.sh \
  scripts/review-c154d.sh scripts/test-c154d.sh scripts/vet-c154d.sh \
  scripts/commit-c154d.sh
git diff --cached --check
git commit -m "feat(schema): add rolling deployment coordinator"
git push origin HEAD:master
