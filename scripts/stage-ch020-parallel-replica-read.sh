#!/usr/bin/env bash
set -eu
git add Makefile README.md INSPIRATION_BACKLOG.md BENCHMARK.md CH020_PARALLEL_REPLICA_READ.md \
  hat/hatPipeline/ch020_parallel_replica_read.go hat/hatPipeline/ch020_parallel_replica_read_test.go \
  scripts/benchmark-ch020-baseline.sh scripts/benchmark-ch020-parallel-replica-read.sh \
  scripts/format-ch020-parallel-replica-read.sh scripts/test-ch020-parallel-replica-read.sh \
  scripts/test-ch020-package.sh scripts/race-ch020-parallel-replica-read.sh \
  scripts/vet-ch020-parallel-replica-read.sh scripts/verify-ch020-parallel-replica-read-docs.sh \
  scripts/review-ch020-parallel-replica-read.sh scripts/stage-ch020-parallel-replica-read.sh \
  scripts/commit-ch020-parallel-replica-read.sh scripts/push-ch020-parallel-replica-read.sh
