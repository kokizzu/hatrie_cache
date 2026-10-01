#!/usr/bin/env bash
set -euo pipefail

git diff --check
printf '%s\n' 'feature worktree status:'
git status --short
printf '%s\n' 'feature diff stat:'
git diff --stat -- \
    BENCHMARK.md \
    Makefile \
    PRODUCT_IDEA_GAPS.md \
    README.md \
    TU06_REPLICA_READ_ONLY.md \
    hat/hatCache/command.go \
    hat/hatCache/local_partition.go \
    hat/hatCache/main.go \
    hat/hatCache/replica_read_only.go \
    hat/hatCache/tu06_replica_read_only_baseline_benchmark_test.go \
    hat/hatCache/tu06_replica_read_only_test.go \
    scripts/format-round39-replica-read-only.sh \
    scripts/test-round39-replica-read-only.sh \
    scripts/verify-round39-replica-read-only.sh
