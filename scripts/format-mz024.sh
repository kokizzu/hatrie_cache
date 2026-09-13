#!/usr/bin/env bash
set -euo pipefail

gofmt -w cmd/hatrie-cache/journal_cursor_config_test.go cmd/hatrie-cache/main.go hat/hatCache/journal.go hat/hatCache/journal_cursor.go hat/hatCache/journal_cursor_test.go hat/hatCache/journal_cursor_benchmark_test.go hat/hatCache/monitoring.go
