#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
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
  hat/hatBackup/c241_chunked_backup_benchmark_test.go
