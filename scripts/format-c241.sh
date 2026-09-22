#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatBackup/model.go \
	hat/hatCache/backup_bundle.go \
	hat/hatCache/backup_repository.go \
	hat/hatCache/c241_backup_chunk_dedup_test.go \
	hat/hatCache/c241_backup_chunk_dedup_benchmark_test.go
