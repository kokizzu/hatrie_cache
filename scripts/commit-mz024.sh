#!/usr/bin/env bash
set -euo pipefail

git add -- Makefile README.md ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md INSPIRATION_BACKLOG.md MZ024_JOURNAL_CURSOR.md cmd/hatrie-cache/main.go cmd/hatrie-cache/journal_cursor_config_test.go hat/hatCache/journal.go hat/hatCache/journal_cursor.go hat/hatCache/journal_cursor_test.go hat/hatCache/journal_cursor_benchmark_test.go hat/hatCache/monitoring.go scripts/benchmark-mz024.sh scripts/check-mz024.sh scripts/commit-mz024.sh scripts/format-mz024.sh scripts/push-mz024.sh scripts/race-mz024.sh scripts/test-mz024-red.sh scripts/test-mz024-cli-red.sh scripts/test-mz024.sh scripts/vet-mz024.sh
git commit -m 'feat: add resumable journal tail cursors'
