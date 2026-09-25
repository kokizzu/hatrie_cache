#!/usr/bin/env bash
set -euo pipefail

git add -- \
    BENCHMARK.md \
    CH025_COMPACTION_PRIORITY.md \
    ENGINE_IDEAS.md \
    Makefile \
    hat/hatStorage/ch025_compaction_priority.go \
    hat/hatStorage/ch025_compaction_priority_baseline_benchmark_test.go \
    hat/hatStorage/ch025_compaction_priority_test.go \
    hat/hatStorage/compaction_scheduler.go \
    scripts/benchmark-ch025-priority-baseline.sh \
    scripts/benchmark-ch025-priority.sh \
    scripts/commit-ch025-priority.sh \
    scripts/format-ch025-priority.sh \
    scripts/push-ch025-priority.sh \
    scripts/race-ch025-priority.sh \
    scripts/stage-ch025-priority.sh \
    scripts/test-ch025-package.sh \
    scripts/test-ch025-priority.sh \
    scripts/vet-ch025-priority.sh

git diff --cached --check
git diff --cached --stat
