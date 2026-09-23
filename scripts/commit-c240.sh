#!/usr/bin/env bash
set -euo pipefail

git add Makefile README.md BENCHMARK.md ADOPTED_QUERY_ENGINE_IDEAS.md INSPIRATION_ROUND2.md C240_READ_ONLY_BACKUP_ATTACHMENT.md
git add hat/hatBackup/read_only_attachment.go hat/hatBackup/c240_read_only_attachment_test.go hat/hatBackup/c240_read_only_attachment_benchmark_test.go
git add hat/hatCache/pebble_store.go hat/hatCache/c240_read_only_pebble_test.go
git add scripts/benchmark-c240.sh scripts/commit-c240.sh scripts/format-c240.sh scripts/push-c240.sh scripts/race-c240.sh scripts/status-c240.sh scripts/test-c240-package.sh scripts/test-c240.sh scripts/verify-c240-docs.sh scripts/vet-c240.sh
git diff --cached --check
git commit -m "Add read-only backup attachments"
