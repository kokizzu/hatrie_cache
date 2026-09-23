#!/usr/bin/env bash
set -euo pipefail

git add Makefile README.md BENCHMARK.md INSPIRATION_ROUND2.md T211_CONFIGURABLE_WAL_SYNC.md \
  hat/hatCache/journal.go \
  hat/hatCache/t211_wal_sync_baseline_benchmark_test.go \
  hat/hatCache/t211_wal_sync_benchmark_test.go \
  hat/hatCache/t211_wal_sync_test.go \
  hat/hatCache/t211_wal_sync_validation_test.go \
  hat/hatJournal/journal.go \
  hat/hatJournal/space_sync_policy.go \
  scripts/benchmark-t211-before.sh scripts/benchmark-t211.sh \
  scripts/format-t211.sh scripts/test-t211.sh scripts/test-t211-package.sh \
  scripts/race-t211.sh scripts/vet-t211.sh scripts/verify-t211-scope.sh \
  scripts/stage-t211.sh scripts/commit-t211.sh scripts/push-t211.sh
