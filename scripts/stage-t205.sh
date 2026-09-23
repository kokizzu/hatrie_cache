#!/usr/bin/env bash
set -euo pipefail

git add \
    BENCHMARK.md \
    INSPIRATION_ROUND2.md \
    Makefile \
    README.md \
    T205_LSN_REPLICATION_METRICS.md \
    hat/hatCache/monitoring.go \
    hat/hatCache/replication.go \
    hat/hatCache/t205_replication_apply_metrics_test.go \
    hat/hatReplication/metrics.go \
    hat/hatReplication/t205_apply_metrics_benchmark_test.go \
    hat/hatReplication/t205_apply_metrics_test.go \
    scripts/benchmark-t205.sh \
    scripts/commit-t205.sh \
    scripts/format-t205.sh \
    scripts/push-t205.sh \
    scripts/race-t205.sh \
    scripts/stage-t205.sh \
    scripts/test-t205-package.sh \
    scripts/test-t205.sh \
    scripts/vet-t205.sh

git diff --cached --check
git diff --cached --name-only
