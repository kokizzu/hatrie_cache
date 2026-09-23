#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatCache/journal.go hat/hatCache/journal_segments.go hat/hatCache/tt_g10_segment_seek_test.go
