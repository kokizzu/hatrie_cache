#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatBackup/model.go \
  hat/hatBackup/consistency.go \
  hat/hatBackup/chain.go \
  hat/hatCache/backup_bundle.go \
  hat/hatCache/backup_repository.go \
  hat/hatCache/backup_attachment.go \
  hat/hatCache/c241_backup_chunk_dedup_test.go \
  hat/hatCache/c241_backup_chunk_dedup_baseline_benchmark_test.go
