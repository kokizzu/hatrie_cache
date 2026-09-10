#!/usr/bin/env bash
set -euo pipefail

git add \
  hat/hatSchema/rolling_schema.go \
  hat/hatSchema/rolling_schema_test.go \
  SCHEMA_ROLLOUT.md \
  INSPIRATION.md \
  README.md \
  Makefile \
  scripts/benchmark-c154-rollout.sh \
  scripts/test-c154-rollout.sh \
  scripts/test-c154-race.sh \
  scripts/vet-c154-rollout.sh \
  scripts/test-c154-regression.sh \
  scripts/test-c154-all.sh \
  scripts/format-c154.sh \
  scripts/commit-c154-rollout.sh
git diff --cached --check
git commit -m 'feat(schema): add rolling deployment state machine'
git push origin master
