#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatReplication/tu39_space_changefeed.go \
  hat/hatReplication/tu39_space_changefeed_test.go \
  hat/hatReplication/tu39_space_changefeed_benchmark_test.go \
  hat/hatReplication/tu39_space_changefeed_baseline_benchmark_test.go
