#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatCache/journal_space_feed.go \
  hat/hatCache/journal_space_feed_test.go \
  hat/hatCache/journal_space_feed_benchmark_test.go
