#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatBackup -count=1
go test -race ./hat/hatBackup -run '^TestCH022' -count=1
go vet ./hat/hatBackup
git diff --check -- \
  Makefile README.md ENGINE_IDEAS.md ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md \
  CH022_INCREMENTAL_PART_BACKUP.md \
  hat/hatBackup/model.go hat/hatBackup/chain.go hat/hatBackup/encryption.go \
  hat/hatBackup/object_store.go \
  hat/hatBackup/ch022_incremental_part_backup_test.go \
  hat/hatBackup/ch022_incremental_part_backup_benchmark_test.go \
  scripts/inspect-ch022-incremental-backup-c203.sh \
  scripts/test-ch022-incremental-part-backup-c203.sh \
  scripts/format-ch022-incremental-part-backup-c203.sh \
  scripts/benchmark-ch022-incremental-part-backup-c203.sh \
  scripts/verify-ch022-incremental-part-backup-c203.sh \
  scripts/stage-ch022-incremental-part-backup-c203.sh \
  scripts/inspect-staged-ch022-incremental-part-backup-c203.sh \
  scripts/commit-ch022-incremental-part-backup-c203.sh \
  scripts/push-ch022-incremental-part-backup-c203.sh
