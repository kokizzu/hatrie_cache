#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatReplication/changefeed_progress.go \
  hat/hatReplication/changefeed_progress_test.go \
  hat/hatReplication/changefeed_progress_benchmark_test.go
