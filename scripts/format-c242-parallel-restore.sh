#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	cmd/hatrie-cli/c242_parallel_restore_test.go \
	cmd/hatrie-cli/main.go \
	hat/hatBackup/restore_files.go \
  hat/hatBackup/restore_files_c242_benchmark_test.go \
  hat/hatBackup/restore_files_c242_test.go \
  hat/hatBackup/reports.go \
  hat/hatCache/backup_repository.go \
  hat/hatCache/backup_restore.go
