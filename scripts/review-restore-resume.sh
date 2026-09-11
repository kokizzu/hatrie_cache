#!/usr/bin/env bash
set -euo pipefail

git status --short
git diff --check -- \
  BENCHMARK.md ENGINE_IDEAS.md Makefile README.md RESTORE_RESUME.md \
  cmd/hatrie-cli/main.go cmd/hatrie-cli/main_test.go \
  hat/hatBackup/reports.go hat/hatBackup/restore_fs.go \
  hat/hatCache/backup_doctor.go hat/hatCache/backup_repository.go \
  hat/hatCache/backup_restore.go hat/hatCache/backup_restore_resume_test.go \
  hat/hatCache/backup_restore_resume_benchmark_test.go \
  scripts/format-restore-resume.sh scripts/benchmark-restore-resume.sh \
  scripts/test-restore-resume.sh scripts/test-restore-resume-cli.sh \
  scripts/test-race-restore-resume.sh scripts/test-restore-resume-broad.sh \
  scripts/vet-restore-resume.sh scripts/review-restore-resume.sh \
  scripts/restore-bundle.sh
