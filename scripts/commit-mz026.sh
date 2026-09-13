#!/usr/bin/env bash
set -eu

git add Makefile README.md BENCHMARK.md INSPIRATION_BACKLOG.md MZ026_SUBSCRIPTION_SNAPSHOT_EXPORT.md hat/hatSql/subscription_snapshot_export.go hat/hatSql/mz026_subscription_snapshot_export_test.go hat/hatSql/mz026_subscription_snapshot_export_benchmark_test.go scripts/test-mz026-snapshot-export.sh scripts/format-mz026.sh scripts/test-mz026-race.sh scripts/test-mz026-full.sh scripts/benchmark-mz026.sh scripts/status-mz026.sh scripts/review-mz026.sh scripts/commit-mz026.sh scripts/push-mz026.sh
git diff --cached --check
git diff --cached --stat
git commit -m 'feat: add exact-frontier subscription export'
