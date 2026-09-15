#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  cmd/hatrie-cli/partition_local_backup_test.go \
  hat/hatBackup/model.go \
  hat/hatCache/backup_partition_restore.go \
  hat/hatCache/backup_partition_restore_benchmark_test.go \
  hat/hatCache/backup_partition_restore_test.go \
  hat/hatCache/backup_restore.go
