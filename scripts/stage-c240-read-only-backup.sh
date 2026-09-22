#!/usr/bin/env bash
set -euo pipefail

git add \
	BENCHMARK.md \
	C240_READ_ONLY_BACKUP_ATTACHMENT.md \
	INSPIRATION_ROUND2.md \
	Makefile \
	README.md \
	hat/hatCache/c240_read_only_backup_test.go \
	hat/hatCache/read_only_backup.go \
	scripts/benchmark-c240-read-only-backup.sh \
	scripts/format-c240.sh \
	scripts/race-c240-read-only-backup.sh \
	scripts/stage-c240-read-only-backup.sh \
	scripts/commit-c240-read-only-backup.sh \
	scripts/push-c240-read-only-backup.sh \
	scripts/test-c240-package.sh \
	scripts/test-c240-read-only-backup.sh \
	scripts/vet-c240-read-only-backup.sh

git diff --cached --check
git diff --cached --stat
