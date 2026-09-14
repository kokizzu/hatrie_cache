#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatBackup/model.go \
  hat/hatBackup/chain.go \
  hat/hatBackup/encryption.go \
  hat/hatBackup/object_store.go \
  hat/hatBackup/ch022_incremental_part_backup_test.go \
  hat/hatBackup/ch022_incremental_part_backup_benchmark_test.go
