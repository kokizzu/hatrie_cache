#!/usr/bin/env bash
set -eu

git add README.md INSPIRATION_BACKLOG.md BENCHMARK.md MZ025_TAIL_HEARTBEAT.md Makefile hat/hatSql/subscription.go hat/hatSql/mz025_tail_heartbeat_test.go hat/hatSql/mz025_tail_heartbeat_benchmark_test.go scripts/benchmark-mz025.sh scripts/format-mz025.sh scripts/status-mz025.sh scripts/test-mz025-full.sh scripts/test-mz025-race.sh scripts/test-mz025.sh scripts/commit-mz025.sh scripts/push-mz025.sh
git commit -m "feat: add subscription heartbeats"
