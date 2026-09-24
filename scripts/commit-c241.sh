#!/usr/bin/env bash
set -euo pipefail

git add \
  BENCHMARK.md \
  C241_INCREMENTAL_CHUNK_DEDUP.md \
  INSPIRATION_ROUND2.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  README.md \
  Makefile \
  hat/hatBackup/model.go \
  hat/hatBackup/encryption.go \
  hat/hatBackup/object_store.go \
  hat/hatBackup/chunked_backup.go \
  hat/hatBackup/chunked_restore.go \
  hat/hatBackup/chain.go \
  hat/hatBackup/object_store_gc.go \
  hat/hatBackup/read_only_attachment.go \
  hat/hatBackup/chunked_attachment.go \
  hat/hatBackup/c241_chunked_backup_test.go \
  hat/hatBackup/c241_chunked_backup_benchmark_test.go \
  scripts/test-c241.sh \
  scripts/format-c241.sh \
  scripts/benchmark-c241.sh \
  scripts/test-c241-package.sh \
  scripts/race-c241.sh \
  scripts/vet-c241.sh \
  scripts/verify-c241-docs.sh \
  scripts/status-c241.sh \
  scripts/commit-c241.sh \
  scripts/push-c241.sh
git commit -m "add incremental backup chunk deduplication"
