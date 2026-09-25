#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatCache/backup_restore.go \
  hat/hatCache/backup_restore_resume_checkpoint.go \
  hat/hatCache/backup_restore_resume_test.go \
  hat/hatCache/backup_restore_resume_checkpoint_benchmark_test.go
