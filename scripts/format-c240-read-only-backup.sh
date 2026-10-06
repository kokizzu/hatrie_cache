#!/usr/bin/env bash
set -euo pipefail
gofmt -w \
	hat/hatBackup/c240_read_only_backup_test.go \
	hat/hatBackup/c240_read_only_backup_benchmark_test.go \
	hat/hatBackup/read_only_backup.go
