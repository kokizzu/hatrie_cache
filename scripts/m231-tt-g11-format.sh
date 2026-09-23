#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatJournal/compress.go hat/hatJournal/journal.go hat/hatCache/journal_zstd_segment_benchmark_test.go hat/hatCache/tt_g11_default_compression_test.go
