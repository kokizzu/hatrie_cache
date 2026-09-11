#!/usr/bin/env bash
set -euo pipefail

git add Makefile README.md BENCHMARK.md ENGINE_IDEAS.md ADOPTED_QUERY_ENGINE_IDEAS.md SNAPSHOT_RESTORE_WORKERS.md \
  hat/hatCache/local_partition.go hat/hatCache/main.go \
  hat/hatCache/snapshot_restore_staged.go \
  hat/hatCache/snapshot_restore_workers_test.go \
  hat/hatCache/local_partition_restore_benchmark_test.go \
  scripts/benchmark-mz017-restore-workers.sh \
  scripts/format-mz017-restore-workers.sh \
  scripts/test-mz017-restore-workers.sh \
  scripts/test-race-mz017-restore-workers.sh \
  scripts/test-mz017-broad.sh scripts/verify-mz017-docs.sh \
  scripts/review-mz017.sh scripts/status-mz017.sh \
  scripts/commit-mz017.sh scripts/push-mz017.sh
git commit -m "feat: bound partition restore workers"
