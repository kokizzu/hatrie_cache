#!/usr/bin/env bash
set -euo pipefail
git add \
	BENCHMARK.md \
	INSPIRATION_ROUND2.md \
	Makefile \
	README.md \
	hat/hatBackup/c240_read_only_backup_benchmark_test.go \
	hat/hatBackup/c240_read_only_backup_test.go \
	hat/hatBackup/read_only_backup.go \
	scripts/benchmark-c240-read-only-backup.sh \
	scripts/commit-c240-read-only-backup.sh \
	scripts/format-c240-read-only-backup.sh \
	scripts/push-c240-read-only-backup.sh \
	scripts/status-c240-read-only-backup.sh \
	scripts/stage-c240-read-only-backup.sh \
	scripts/test-c240-read-only-backup.sh \
	scripts/verify-c240-read-only-backup.sh
git diff --cached --check
git status --short
