#!/usr/bin/env bash
set -euo pipefail
gofmt -w \
  hat/hatBackup/ch022_catalog_integration_test.go \
  hat/hatBackup/ch022_catalog_backup_benchmark_test.go \
  hat/hatBackup/ch022_catalog_backup_baseline_benchmark_test.go \
  hat/hatBackup/encryption.go \
  hat/hatBackup/object_store.go
