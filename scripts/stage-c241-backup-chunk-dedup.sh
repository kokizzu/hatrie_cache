#!/usr/bin/env bash
set -euo pipefail

git diff --check
git add \
	BENCHMARK.md \
	C241_INCREMENTAL_BACKUP_CHUNK_DEDUP.md \
	INSPIRATION_ROUND2.md \
	Makefile \
	README.md \
	hat/hatBackup/model.go \
	hat/hatCache/backup_bundle.go \
	hat/hatCache/backup_repository.go \
	hat/hatCache/c241_backup_chunk_dedup_benchmark_test.go \
	hat/hatCache/c241_backup_chunk_dedup_test.go \
	scripts/benchmark-c241-backup-chunk-dedup.sh \
	scripts/commit-c241-backup-chunk-dedup.sh \
	scripts/format-c241.sh \
	scripts/push-c241-backup-chunk-dedup.sh \
	scripts/race-c241-backup-chunk-dedup.sh \
	scripts/stage-c241-backup-chunk-dedup.sh \
	scripts/test-c241-backup-chunk-dedup.sh \
	scripts/test-c241-package.sh \
	scripts/vet-c241-backup-chunk-dedup.sh
