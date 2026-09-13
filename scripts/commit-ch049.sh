#!/usr/bin/env bash
set -euo pipefail

git add BACKUP_ENCRYPTION.md BENCHMARK.md INSPIRATION_BACKLOG.md Makefile README.md hat/hatBackup/chain.go hat/hatBackup/encryption.go hat/hatBackup/encryption_test.go hat/hatBackup/model.go hat/hatBackup/object_store.go scripts/benchmark-ch049-after.sh scripts/benchmark-ch049-before.sh scripts/check-ch049.sh scripts/commit-ch049.sh scripts/format-ch049.sh scripts/push-ch049.sh scripts/race-ch049.sh scripts/test-ch049-all.sh scripts/test-ch049-encryption-red.sh scripts/test-ch049.sh scripts/vet-ch049.sh
git commit -m "feat: add encrypted object-store backups"
