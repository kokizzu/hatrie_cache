#!/usr/bin/env bash
set -euo pipefail

git add Makefile README.md BENCHMARK.md ENGINE_IDEAS.md SELECTIVE_BACKUP.md hat/hatBackup/model.go hat/hatCache/backup_bundle.go hat/hatCache/backup_repository.go hat/hatCache/backup_selective_test.go hat/hatCache/backup_selective_benchmark_test.go scripts/benchmark-selective-backup.sh scripts/test-selective-backup.sh scripts/format-selective-backup.sh scripts/test-race-selective-backup.sh scripts/test-selective-backup-broad.sh scripts/vet-selective-backup.sh scripts/verify-selective-backup.sh scripts/review-selective-backup.sh scripts/commit-selective-backup.sh scripts/push-selective-backup.sh
git diff --cached --check
git commit -m "feat: add selective snapshot backup scopes"
