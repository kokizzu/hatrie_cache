#!/usr/bin/env bash
set -euo pipefail

git diff --check -- \
  Makefile \
  README.md \
  INSPIRATION.md \
  hat/hatReplication/model.go \
  hat/hatCache/replication.go \
  hat/hatCache/monitoring.go \
  hat/hatCache/replication_lag_test.go \
  scripts

for script in \
  scripts/commit-t051.sh \
  scripts/format-t051.sh \
  scripts/push-t051.sh \
  scripts/review-t051.sh \
  scripts/stage-t051.sh \
  scripts/test-race-t051.sh \
  scripts/test-t051.sh \
  scripts/vet-t051.sh; do
  bash -n "$script"
done
