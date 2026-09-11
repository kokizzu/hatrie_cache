#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatBackup/reports.go \
  hat/hatBackup/restore_fs.go \
  hat/hatCache/backup_doctor.go \
  hat/hatCache/backup_repository.go \
  hat/hatCache/backup_restore.go \
  hat/hatCache/backup_restore_resume_test.go \
  hat/hatCache/backup_restore_resume_benchmark_test.go \
  cmd/hatrie-cli/main.go \
  cmd/hatrie-cli/main_test.go
