#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatCache/journal.go \
  hat/hatCache/journal_segments.go \
  hat/hatCache/journal_zstd_segment_benchmark_test.go \
  hat/hatCache/journal_zstd_segment_test.go \
    hat/hatJournal/compress.go \
    hat/hatJournal/journal.go \
    hat/hatJournal/reader.go \
    api_journal_compression.go \
  cmd/hatrie-cache/main.go \
    cmd/hatrie-cache/main_test.go \
    hat/hatCache/journal_zstd_backup_test.go
