#!/usr/bin/env bash
set -euo pipefail

git status --short
git diff --check
git diff --cached --check
git diff --cached --name-status
git diff --cached --stat
git diff --stat -- \
  hat/hatReplication/model.go \
  hat/hatReplication/metrics.go \
  hat/hatReplication/metrics_test.go \
  hat/hatCache/replication.go \
  hat/hatCache/monitoring.go \
  hat/hatCache/replication_test.go \
  scripts/test-t112.sh \
  scripts/format-t112.sh \
  scripts/benchmark-t112.sh \
  scripts/test-race-t112.sh \
  scripts/vet-t112.sh \
  scripts/review-t112.sh
git diff -- \
  hat/hatReplication/model.go \
  hat/hatReplication/metrics.go \
  hat/hatReplication/metrics_test.go \
  hat/hatCache/replication.go \
  hat/hatCache/monitoring.go \
  hat/hatCache/replication_test.go
rg -n -C 4 'T112|Per-Queue Resident Memory And Timing Metrics|estimated_queued_bytes|queue_wait_millis' \
  INSPIRATION.md README.md REPLICATION_OPERATIONS.md BENCHMARK.md
