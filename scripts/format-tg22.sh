#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatCache/journal.go \
	hat/hatCache/journal_segments.go \
	hat/hatCache/journal_backup_retention.go \
	hat/hatCache/journal_backup_retention_test.go \
	hat/hatCache/journal_backup_retention_benchmark_test.go
