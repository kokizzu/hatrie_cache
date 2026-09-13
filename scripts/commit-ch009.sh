#!/usr/bin/env bash
set -eu

git diff --check
git add \
  BENCHMARK.md \
  CH009_ASYNC_INSERT_BUFFER.md \
  INSPIRATION_BACKLOG.md \
  Makefile \
  README.md \
  hat/hatCache/command_idempotency.go \
  hat/hatCache/ch009_async_insert_buffer.go \
  hat/hatCache/ch009_async_insert_buffer_benchmark_test.go \
  hat/hatCache/ch009_async_insert_buffer_test.go \
  scripts/benchmark-ch009-after.sh \
  scripts/benchmark-ch009-before.sh \
  scripts/commit-ch009.sh \
  scripts/format-ch009.sh \
  scripts/push-ch009.sh \
  scripts/status-ch009.sh \
  scripts/test-ch009-race.sh \
  scripts/test-ch009.sh
git diff --cached --check
git commit -m "feat: add bounded async insert buffer"
