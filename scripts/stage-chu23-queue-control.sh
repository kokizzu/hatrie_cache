#!/usr/bin/env bash
set -euo pipefail

git add -- \
  BENCHMARK.md \
  CHU23_ASYNC_INSERT_QUEUE.md \
  INSPIRATION.md \
  Makefile \
  README.md \
  hat/hatCache/async_command.go \
  hat/hatCache/async_command_http.go \
  hat/hatCache/async_command_queue.go \
  hat/hatCache/async_command_queue_http.go \
  hat/hatCache/chu23_async_command_queue_benchmark_test.go \
  hat/hatCache/chu23_async_command_queue_test.go \
  hat/hatCache/journal.go \
  hat/hatCache/monitoring.go \
  scripts/benchmark-chu23-queue-control.sh \
  scripts/commit-chu23-queue-control.sh \
  scripts/format-chu23-queue-control.sh \
  scripts/push-chu23-queue-control.sh \
  scripts/race-chu23-queue-control.sh \
  scripts/status-chu23-queue-control.sh \
  scripts/stage-chu23-queue-control.sh \
  scripts/test-chu23-async-regression.sh \
  scripts/test-chu23-queue-control.sh \
  scripts/vet-chu23-queue-control.sh
