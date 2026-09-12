#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatReplication/changefeed_checkpoint.go \
  hat/hatReplication/changefeed_checkpoint_test.go \
  hat/hatReplication/changefeed_checkpoint_benchmark_test.go
